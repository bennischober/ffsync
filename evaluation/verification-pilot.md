# Verification pilot: property tests and bounded proofs (2026-11-08)

This pilot asked whether property testing (proptest) and bounded model
checking (Kani) find anything the example-based suites, the fuzzers and two
bug hunts missed, and which TLA+ model would be worth building next.

**Result in short:**

- All 27 properties pass on Windows and on Linux (WSL Ubuntu 20.04, run
  locally 2026-11-08 together with the rest of `cargo test`: every suite
  passes). None fails on current code.
- Planted bugs make them fail, so they have teeth. Mutation check: a session
  expectation that ignores `--no-delete`, an off-by-one in the reference
  glob's class ranges, and a halved mtime tolerance each produced a failing
  property with a shrunk counterexample.
- Writing the properties surfaced one real bug next to them: a hostile tree
  sender could crash the receiver with an out-of-range mtime. It is fixed in
  its own commit, with a regression test.
- A second suite, the hostile-sender properties, drives the real tree
  receiver against a sender whose frames are mutated in flight. It found a
  second real bug: the receiver did not check that a file conversation was
  for the path it expected, so a lying manifest got one file's bytes
  committed under another name. Fixed, with a regression test.
- Three spec and API observations are recorded below. Observation 3 (a
  manifest listing a file `a` and an entry `a/b`) is now fixed; the other
  two need no change.
- A TLA+ model of the §16.8 parallel-session barrier passes against the
  reference sender (with one fault) and against an arbitrary sender. Each
  of its four receiver guards, switched off, gives a counterexample. It
  found one spec mismatch, now amended: §16.8 said the sender reports
  ERROR on the control connection after a worker failure, but it closes
  the connection without TREE_END instead. Both fail closed. See "TLA+
  model of the parallel-session barrier".
- The Kani harnesses type-check but have **not been run**. Kani 0.68
  installs in WSL, but its CBMC build needs glibc 2.34 or newer, and the
  local distro (Ubuntu 20.04) has 2.31. See "Kani harnesses".

## How to run

```bash
cargo test --test properties
```

It runs in about 1.3 s (debug build, 27 tests in parallel) and is part of
plain `cargo test`, so CI runs it.

The suite uses no failure persistence: integration tests have no `lib.rs`
beside them, and regression files would be generated artifacts. A failing
case prints its shrunk input; record it here.

```bash
cargo kani
```

Kani runs on Linux or macOS only; on Windows, run it in WSL. One-time
setup: `cargo install --locked kani-verifier && cargo kani setup`. The
WSL distro also needs a C toolchain (`sudo apt install build-essential`)
before the crate builds at all, and Kani 0.68 needs glibc 2.34 or newer
(Ubuntu 22.04+; 20.04 is too old). It is not in CI.

The harnesses were type-checked on Windows against a stub `kani` crate:
`cargo rustc --lib --profile=check -- --cfg kani --extern kani=<stub>`.
Without the stub the same build fails with 19 unresolved `kani` paths, which
shows the `#[cfg(kani)]` modules really compile. The unwind bounds are
estimates and have not been checked: if `cargo kani` reports an unwinding
assertion failure, raise the harness's `#[kani::unwind(N)]`.

## Properties (`tests/properties.rs`)

### Tree triage (pure model, §16.3/§16.4)

The generator builds two consistent trees independently over the names
`a`/`b`/`c`, at most three levels deep. Every ancestor is a dir and no file
has children. Using a three-letter alphabet makes shared paths, kind
conflicts and receiver-only subtrees common. An optional "copy" flag per
shared file makes the receiver's file identical to the sender's (same
content and mtime), which exercises the Skip path.

| Property | Cases | Checks | Found |
|---|---|---|---|
| `triage_is_deterministic_and_order_independent` | 512 | Manifest order and triage results don't depend on insertion order. The sender's conversation set (its own entries) equals the receiver's (the union). | nothing |
| `triage_classification` | 512 | Delete happens iff the path is receiver-only, never for a path the sender has. Conflict happens iff kinds differ, and carries the sender's kind. A file pair skips iff size and mtime (100 ns) agree and `--checksum` is off. | nothing |
| `second_triage_after_sync_is_all_skip` | 512 | Apply one session to the model, with the receiver storing mtimes exactly or at the NTFS 100 ns tick. A second triage is then all Skip, plus gated Deletes only under `--no-delete`. | nothing |
| `no_delete_removes_only_conflict_subtrees` | 512 | Under `--no-delete`, every receiver-only path survives unchanged unless it lies under a sender file (a resolved kind conflict). | nothing — this is the documented exception (§16.3, "Conflict resolution is not gated") |
| `reverse_sort_is_children_first` | 512 | The delete pass's reverse lexicographic order puts every descendant before its ancestor. | nothing |

### Real tree sessions (TCP loopback)

| Property | Cases | Checks | Found |
|---|---|---|---|
| `tree_session_converges` | 24 | Random sender and receiver trees, random `--no-delete`, `--parallel 1..=3`, and `--eager`. The receiver ends exactly where the model says (paths, kinds, contents). A second triage over the two real scans is all Skip. | nothing |

### Excludes (§16.7)

| Property | Cases | Checks | Found |
|---|---|---|---|
| `exclude_pattern_matches_reference` | 2048 | `ExcludePattern::new` and `matches` against an independent recursive matcher, for files and dirs: validity and match verdict agree. Tokens include the edge cases `[`, `]`, `[]a]`, `[!]`, `**`, `!`, `!!`, and leading or trailing `/`. | nothing |
| `exclude_set_matches_reference` | 2048 | First match wins, `!` re-includes, and `matches_path_or_ancestor` (the delete-pass guard) equals "the path or any ancestor dir is excluded". | nothing |
| `scan_with_excludes_prunes_like_the_guard` | 32 | On a real tree, the scan lists an entry iff `!matches_path_or_ancestor`. Pruning and the guard are the same rule. | nothing |

### Path safety (§16.2)

| Property | Cases | Checks | Found |
|---|---|---|---|
| `validate_rel_path_is_the_spec` | 4096 | Accepts exactly the paths that are non-empty, contain no `\`, and have no empty, `.` or `..` component. Inputs mix drive prefixes, device names, trailing dots and spaces, NUL, and the `\\?\` prefix. | nothing |
| `accepted_paths_stay_under_root` | 4096 | Whenever both `validate_rel_path` and `check_local_name` accept a path, joining it under the root adds exactly one Normal component per wire segment: no `..`, no prefix, no root, no collapse. | nothing |
| `manifest_from_wire_admits_only_safe_entries` | 4096 | Accepts iff every path validates, no path is a sidecar, there are no duplicates, every kind is known, and dirs have size 0. | nothing (see observation 3) |
| `link_ancestor_finds_the_first_link` | 16 | On real trees with dir links (junctions on Windows, symlinks on unix), it reports the shallowest proper ancestor that is a link. A shared per-prefix cache never changes an answer. | nothing |

### mtimes (D23)

| Property | Cases | Checks | Found |
|---|---|---|---|
| `mtimes_match_is_the_100ns_bucket` | 4096 | It is an equivalence relation, equal to "same seconds and same `nanos / 100`". A match implies the times differ by less than 100 ns. NTFS truncation still matches the original. One tick apart never matches. | nothing (see observation 1) |

### Chunk coverage and the plan model (§5/§6)

| Property | Cases | Checks | Found |
|---|---|---|---|
| `cdc_chunks_tile_the_file` | 48 | For files up to about 500 KiB with random sane CDC parameters: chunks tile `[0, size)` with no gap or overlap, respect min and max (min except for the last chunk), and hash their own bytes. The chunk count fits the receiver's plan cap (`2·size/min + 2`). The layout is identical across hash-worker counts. | nothing |
| `fixed_chunks_tile_the_file` | 48 | The same tiling holds for `FixedChunker` through `process_file`. | nothing |
| `plan_reconstructs_the_source` | 48 | Old file plus random splice edits. Each new chunk is Local when its hash is in the old file's manifest, otherwise Remote. Every Local source lies inside the old file, and executing the plan reproduces the new file byte for byte. | nothing |

The 256 MiB region size (`REGION_SIZE`) is too large for a property test,
so region stitching stays covered by the bench fingerprints and the
existing pipeline tests.

### Wire codecs (§3/§4/§8/§16)

| Property | Cases | Checks | Found |
|---|---|---|---|
| `frame_round_trip_and_truncation` | 1024 | Encode then read is the identity, and `encoded_len` is exact. Every strict prefix fails with Eof (empty prefix) or Truncated, never with a decoded frame. | nothing |
| `read_frame_never_panics` | 1024 | On arbitrary bytes, an accepted frame is exactly the bytes its header claims. | nothing |
| `hello_round_trip`, `hello_rejects_unknown_flag_bits` | 1024 | Round trip. Any unknown flag bit is refused (strict codec). | nothing |
| `tree_begin_round_trip`, `tree_begin_rejects_unknown_flag_bits` | 1024 | Round trip up to the flag-gated defaults (see observation 2). Unknown bits are refused. | nothing (see observation 2) |
| `decoders_are_canonical` | 1024 | Any one-byte mutation of a valid HELLO, TREE_BEGIN or plan entry that still decodes re-encodes to the same bytes. One message never has two wire forms. | nothing |
| `small_messages_round_trip` | 1024 | MANIFEST_CHUNK, both plan entries, TREE_WORKER, TREE_FILE, TREE_END, ERROR and DATA all round-trip. | nothing |
| `commit_ack_outcome` | 1024 | The D24 byte round-trips. An empty payload means a pre-D24 receiver; any other payload is malformed. | nothing |
| `decoders_never_panic` | 1024 | Every decoder accepts arbitrary payloads of its own frame type without panicking. | nothing |

## Kani harnesses (`#[cfg(kani)] mod verification`)

| Harness | File | Proves (within its bound) |
|---|---|---|
| `read_frame_is_total_and_exact` | `src/protocol/framing.rs` | Covers every input of at most 9 bytes, so every length field is explored, including over-`MAX_PAYLOAD` and longer than the input. No panic and no overflow. An Ok result is exactly the claimed bytes. Eof only on empty input. Io only for the oversize refusal. |
| `hello_decode_is_total_and_strict` | `src/protocol/message.rs` | Covers every payload with the magic plus up to 42 symbolic bytes, which reaches the Ok path with paths of up to 3 bytes. No panic. Accepted HELLOs have only known flag bits, a valid role, and no trailing bytes. |
| `hello_decode_rejects_short_payloads` | `src/protocol/message.rs` | Every payload of at most 8 bytes is refused without panicking. |
| `commit_ack_decode_is_exact` | `src/protocol/message.rs` | Exactly `[]`, `[0]` and `[1]` decode, each to its own meaning. |
| `validated_paths_cannot_escape_the_root` | `src/tree.rs` | For every UTF-8 string of at most 4 bytes that `validate_rel_path` accepts, `/r` joined with it has only Normal components, one per segment. This is the unix half. On Windows, `check_local_name` carries the drive-prefix case, which `accepted_paths_stay_under_root` covers by sampling. |

Status: written and type-checked; **not run**. A first attempt
(2026-11-08) installed `kani-verifier` 0.68.0 in WSL, but its bundled
toolchain needs glibc 2.34 or newer and the distro is Ubuntu 20.04
(glibc 2.31). The run needs Ubuntu 22.04 or newer: another WSL distro, or
a container such as `ubuntu:24.04`. That run will show whether the unwind
bounds are enough and how long CBMC takes on the `format!` paths in
`validate_rel_path`'s error branch.

## Hostile-sender properties

`tests/hostile_sender.rs` runs the **real** tree receiver (`tree_receive`,
and through it `sync::receive`) against the real `tree_send`, over real
TCP loopback. Every frame the sender writes passes through a proxy,
`MutatingStream`, which applies one scripted mutation per case to the nth
frame of one sender frame type. The receiver sees only the result.

```bash
cargo test --test hostile_sender
PROPTEST_CASES=1500 cargo test --test hostile_sender
```

The default is 64 cases, about 5 s in a debug build, so plain
`cargo test` and CI run it. The env override runs a longer search:
1500 cases take about 2 minutes and pass on current code, on Windows and
on Linux (WSL, 120 s, 2026-11-08). The finding
below shows why the long search matters.

**Mutations.** Generic ones work on any frame: flip a byte, drop the
frame, duplicate it, cut the connection after `keep` bytes, or inject an
arbitrary frame (type `0x00..=0x12`, random payload). Lies decode the
frame, change one field and re-encode it, so the frame stays well formed:

- TREE_BEGIN: an entry's mtime, size, kind or mode; rename an entry; drop
  one; add a child under a file; flip flags; a bad `parallel` count;
- per-file HELLO: size, mtime, CDC parameters, flags, role, version, path;
- PLAN entries: target offset, length, source, hash, or swap two entries;
- DATA: a wrong hash, a zstd bomb (8 MiB of zeros), another chunk's bytes;
- TREE_FILE: name another path; TREE_WORKER: a wrong index.

Sizes are drawn from boundary values, not the GiB–TiB middle, so the
receiver's pre-allocation cannot fill the disk. A mutation counts as
applied only if it changed the bytes on the wire.

**Fixture.** Up to six paths (`a`, `b`, `c`, `d/x`, `d/y`, `e/f/z`), each
present or absent on the sender, empty, small or 40–120 KiB. The receiver
holds nothing, the same file, an edited copy, or other content at each
path, and sometimes a file `d` where the sender has a directory. Each case
also draws `--no-delete`, `--parallel 1..=3` and `--eager`.

**Watchdog.** Both peers run in their own threads under `catch_unwind`.
After 1.5 s without sender I/O the watchdog shuts down every socket; a peer
that has not returned 20 s later is a hang finding.

**What must hold** (oracles checked after every case):

1. Neither peer panics or hangs.
2. Nothing outside the receiver's root changes: a canary file beside it
   stays intact and the sender's tree is untouched.
3. Every receiver file ends as its old content or as the sender's real
   content at **that same path**.
4. A file disappears only through the delete pass of a session the
   receiver reported as successful (without `--no-delete` on either side,
   and only if the accepted TREE_BEGIN does not list it), or through the
   documented kind-conflict removal that TREE_BEGIN asked for.
5. A receiver that reports success has converged on every file in the
   TREE_BEGIN it accepted. A lie must fail loudly, not pass silently.
6. Control: when the mutation never applied, both peers succeed and the
   tree converges.

`mutating_stream_forwards_honest_frames_unchanged` checks the proxy itself:
with no mutation, a `--parallel 2` session converges, nothing counts as
applied, and the watchdog never fires.

## Findings

### Bug (fixed): the tree receiver did not check which file a conversation was for

- **Where:** `sync::receive` validated the per-file HELLO's version and
  role but never compared its `path` with the conversation the receiver
  expected. The check now sits right after `validate_hello`
  (`src/protocol/sync.rs:1063`), fed by `run_one`
  (`src/protocol/tree_sync.rs:1259`). Protocol.md §16.4 already said the
  receiver "fails loudly on the first unexpected HELLO", so the code
  deviated from the spec.
- **Minimal example** (shrunk by proptest): the sender has two empty files
  `c` and `d/x`; the receiver is empty. The mutation adds an entry `d/q` to
  the sender's TREE_BEGIN. Both sides triage the same lying manifest, so the
  receiver expects conversations for `c`, `d/q`, `d/x`. The real sender
  only knows `c` and `d/x`. The receiver ran its `d/q` conversation on the
  HELLO for `d/x`, committed `d/x`'s content as `d/q`, and only then failed
  on the next frame ("unexpected frame type 0x0D"). Oracle 3 fired:
  `d/q` held content the sender never had at that path. With a non-empty
  file the same desync writes one file's bytes under another name.
- **Impact:** only a broken or hostile sender triggers it, and the session
  still failed. But a file was written that should never have existed, and
  in the parallel mode the same gap let a TREE_FILE/HELLO mismatch through.
- **Fix:** `ReceiveRequest::expect_peer_path`. Tree conversations set it to
  the expected wire path (sequential) or the path the preceding TREE_FILE
  named (parallel §16.8). A mismatch is refused with ERROR `BadFrame` before
  anything is written. Single-file mode passes `None`: there the sender's
  path is only a display name. Protocol.md §16.4 now spells out that
  "unexpected" is checked by name. Regression test:
  `tree_receiver_refuses_hello_for_another_path` in `tests/tree.rs`.
- **How it was found:** the first full run after the harness worked. It
  failed after 82 passing cases, so the default 64 cases would not have
  caught it reliably. Run the long search (`PROPTEST_CASES=1500`) after
  changes to the tree receiver.


### Harness finding (fixed in the harness): a "sender hang" that the CLI cannot have

- **What failed:** one default run reported `HANG: the sender did not
  return 20s after its sockets were cut`. The receiver had refused a
  worker that announced index 16 (`--parallel 3`, a TREE_WORKER lie). The
  next run, with different random cases, passed.
- **Why:** it depends on timing. When the receiver accepts the lying worker
  first, it refuses it and returns while the other worker's connection is
  still unaccepted in the listener backlog. The harness held its own
  reference to the listener until the case ended, so that connection was
  never reset. On Windows a local `shutdown` does not wake a read on a
  connection that no peer socket ever owned, so the sender's TREE_WORKER
  ACK read blocked until its 30 s read timeout, past the 20 s budget.
  A standalone probe on Windows confirmed both halves. A blocked read on
  a never-accepted connection ignores a local `shutdown(Both)` and waits
  out its timeout. Closing the listener fails that read at once with
  `ConnectionReset`.
- **Why it is not an ffsync bug:** the CLI receiver's listener closes when
  the session ends (the process exits). That resets the pending
  connection, and the sender's worker fails at once.
- **Fix (harness only):** the receiver thread now owns the listener, so it
  closes when `tree_receive` returns, as in the CLI. The watchdog's
  `register` also checks `killed` under the lock `kill` holds, which
  closes a smaller race of the same kind.

### Bug (fixed): a hostile tree sender could crash the receiver with an mtime

- **Where:**
  - In tree mode, `src/protocol/sync.rs:1077` copies the sender's HELLO
    `file_mtime`/`file_mtime_nanos` into `apply_mtime` without checking
    them.
  - At commit, `apply_commit_mtime` (`src/protocol/sync.rs:2140`) built
    `UNIX_EPOCH + Duration::new(secs, nanos)`.
- **Minimal example:** a HELLO with `file_mtime = u64::MAX` and
  `file_mtime_nanos = 0`. After the file was written and renamed, the
  receiver panicked with "overflow when adding duration to `SystemTime`".
- **A second symptom:** a nanos value ≥ 10⁹ was silently carried into the
  seconds. `u32::MAX` nanos gave an mtime about 4 s later than the sender's,
  so that file could never take the fast path.
- **Fix:** commit 445fbe4. Both cases now return `InvalidData`, which
  `apply_commit_metadata` already turns into a warning, and the file keeps
  its own mtime. The regression test is
  `commit_metadata_survives_unrepresentable_peer_mtimes`. Protocol.md §16.6
  is amended to match.
- **How it was found:** not by a failing property. It surfaced while the
  mtime properties were being written, from tracing where wire mtimes turn
  back into `SystemTime`, and was then confirmed with a failing unit test.
  A session-level property would need a scripted hostile sender, which the
  suite does not have (see "Next").

### Observation 1: the hand-off's mtime rule was looser than the code

The task described `mtimes_match` as "the times differ by less than
100 ns". The code (`src/tree.rs:823`) and Protocol.md §4 say "same seconds
and same `nanos / 100`". These differ: 199 ns and 200 ns are 1 ns apart but
do not match.

The bucket rule is the correct one, because NTFS truncates to 100 ns and the
bucket is exactly what truncation preserves. The property states the
bucket rule plus the implication "match ⇒ difference < 100 ns". No change
needed.

### Observation 2: TREE_BEGIN encoding is lossy, and its count bound is asymmetric

- `TreeBegin::into_frame` silently drops data that its flags don't announce:
  - `parallel` without `PARALLEL`;
  - `excludes` without `EXCLUDES`;
  - `mode` without `METADATA`.

  Decoding then fills in the defaults. The round-trip property therefore
  compares against a normalized value (`tree_begin_normalized`). This is
  intended, since the flags are authoritative, but a caller that sets the
  data and forgets the flag gets no error.
- The decoder bounds the pattern count by "at least 3 bytes per pattern"
  (`src/protocol/message.rs:668`). The encoder writes an empty pattern as
  2 bytes, so a TREE_BEGIN carrying 12 or more empty patterns encodes but
  does not decode.

  This is harmless: an empty pattern is invalid (`ExcludePattern::new`
  rejects it) and the CLI validates patterns before sending. The
  round-trip generator therefore produces non-empty patterns only. This is
  recorded here so the restriction is not mistaken for a weakened property.

### Observation 3 (fixed): `manifest_from_wire` did not check prefix consistency

`manifest_from_wire` (`src/protocol/tree_sync.rs:180`) accepted a manifest
that lists both a file `a` and an entry `a/b`. A scan can never produce
this, so only a broken or hostile sender would send it.

On such a manifest the receiver wrote `a`, then failed loudly when it
reached `a/b`, because the parent is a file. Nothing escaped the root and
nothing was silently lost, but files were written before the session
failed.

- **Fix:** the decoder now refuses the manifest up front, like the
  duplicate-path check: after collecting the entries it looks up every
  proper ancestor of every path and fails if one is a file entry. Wire
  order is not trusted, so the check runs after the loop. A path under a
  listed directory, or under no listed entry, is still fine. Protocol.md
  §16.2 is amended to match.
- **Tests:** `tree_wire_manifest_refuses_entries_under_a_file` (decoder,
  both orders, any depth, plus the allowed look-alikes `ab/c` and `a-b`)
  and `tree_receiver_refuses_a_manifest_with_an_entry_under_a_file` (a
  scripted sender: the receiver answers ERROR and writes nothing).
- **The property had no teeth here at first.**
  `manifest_from_wire_admits_only_safe_entries` gained the
  ancestor rule in its reference, but with the check disabled it still
  passed 4096 cases: its paths were drawn only from
  `hostile_path_strategy`, so a manifest whose entries were all valid
  *and* contained a file/child pair almost never occurred. Its paths now
  also come from `wire_path_strategy`. With the check disabled it then
  fails with a shrunk counterexample; with the check in place it passes.

## TLA+ model of the parallel-session barrier

`concept/tla/ParallelSession.tla` (about 500 lines; the estimate below
said 150) models the conversation phase of a §16.8 session at frame
level. It starts after the worker handshake. Every connection carries a
FIFO queue in each direction, plus open/closed flags (EOF comes only
after the last queued frame) and a reset flag (a reset drops queued
frames and turns reads into errors). Each file conversation is reduced
to TREE_FILE + BODY from the sender and COMMIT_ACK or ERROR back; the
receiver commits when it acks. The reference sender's work queue, its
`stop` flag, its join and its TREE_END are modelled as implemented, and
so are the receiver's `remaining` set, `first_error` + `cancel` (workers),
`session_error` (control thread), the `cancel` check before each read
(a thread already blocked in a read is not woken), the join, the
decision, the `sender_end_consumed` gate, and the delete pass. The
receiver's control thread differs from its workers, and the model keeps
that difference: a membership failure there breaks without an ERROR
frame and without setting `cancel`. `concept/tla/README.md` has the run
commands.

The faults are a connection reset, a sender worker that dies without
raising `stop` (a panic), and a local failure inside a conversation on
either side. Fairness covers progress actions only, never faults. So
`Termination` means every behavior ends without needing a *further*
fault, such as a socket timeout, to break a hang.

Checked with TLC 2.19 (tla2tools v1.7.4), 16 workers:

| Config | Result | Distinct states | Time |
|---|---|---|---|
| `Honest`: reference sender, K = 3, 3 files, ≤ 1 fault | no error: TypeOK, ExactlyOnce, NoStranger, DeleteSafe, FailClosed, SenderTruth, DecideClean, Termination, no deadlock | 322,874 (depth 30) | 17 s |
| `Hostile`: arbitrary sender, K = 2, 2 files + a stranger path, 5 frames, ≤ 1 fault | no error: TypeOK, ExactlyOnce, NoStranger, DeleteSafe, DecideClean | 3,176,664 (depth 21) | 18 s |
| `Witness` (`-continue`) | all three witnesses violated, as intended: success, a failure caught only by the decision, and the split outcome are all reachable | 322,874 | 6 s |
| `NoBarrierGate` | **Termination violated**. With zero faults the sender waits for the delete report while the receiver blocks on a second TREE_END read. This is exactly the deadlock the gate's comment describes | 322,808 | 12 s |
| `NoJoin` | **DeleteSafe violated** in 13 steps. A worker takes f1; the control connection takes and commits f2, then reads TREE_END; the decision runs before the join; the files are deleted while f1 is still uncommitted | — | 19 s |
| `NoMembership` | **NoStranger and ExactlyOnce violated**: a path outside the set, or the same path twice, gets committed | 3,650,266 | 3 min (`-continue`) |
| `NoRemaining` | **DeleteSafe violated** in 8 steps. The sender closes the worker connection and sends only TREE_END on the control connection; nothing is committed, and the deletes run | — | 6 s |

What the model shows:

- **Every guard is needed, but most of them only against a hostile
  sender.** The mutations were also run against the reference sender.
  Without the membership check or the empty-set check it still passes:
  the reference sender never repeats a path, and it sends TREE_END only
  after every conversation was acked. So those two guards defend against
  a lying peer, not against our own concurrency. That makes them
  hostile-peer guards, like the §16.2 path checks. The barrier gate is needed against the
  reference sender itself. The join is needed against the reference
  sender only for `DecideClean`: without it a reset on a worker
  connection that arrives after the decision is ignored, though with an
  honest peer every file has committed by then. Against a hostile sender
  the join is load-bearing for `DeleteSafe`.
- **Spec mismatch: the ERROR after a worker failure (amended).** §16.8
  said "the sender then reports ERROR on the control connection". The
  reference sender does that only when the failure happened on the
  control connection itself. After a worker failure it joins and returns,
  and the CLI closes the control connection without TREE_END. The model
  checks the behavior as implemented: the receiver fails closed on the
  missing TREE_END. The `NoQuietFailure` witness trace is this path: a
  worker panics, the sender joins and closes, and the receiver decides
  "fail" on the missing TREE_END. The spec
  text now describes what the code does. Sending an ERROR there would
  only change the receiver's error message.
- **No cancel from the receiver's control thread.** A failure on the
  control connection does not set `cancel`, so the receiver's workers
  keep serving until the sender (whose `stop` is now raised) closes their
  connections. The cost is wasted work, not a hang: `Termination` holds.
  No change made.
- **The split outcome is inherent.** If the connection resets after the
  receiver sent its delete report, the receiver succeeded and the sender
  reports failure. The witness reaches it. No protocol over a single
  final message can rule this out (the two-generals problem). It is safe
  because a retry converges: the deletes have already happened, and the
  re-run is a fast-path no-op.

What it does not cover: the TREE_WORKER handshake and worker
authentication, the inner steps of a conversation (§4–§8 are sequential
and covered by the hostile-sender suite), QUIC teardown (§17.6, see
below), and more than K = 3 connections or 3 files. The guards do not
depend on file count beyond "at least two, on different connections".

## Mmap truncation (M1), reproduced

The bug hunts left one item open: the scan memory-maps the file
(`process_file`, the crate's only `unsafe` block), and another process may
truncate it mid-scan. A scratch program (not in the repo) wrote a 1 GiB
file, started `process_file`, truncated the file to 4 KiB after the first
chunk event, then drained the pipeline.

| Platform | Result |
|---|---|
| Linux (WSL Ubuntu 20.04) | The process dies with SIGBUS (exit status 135) right after the first chunk event. |
| Windows 11 | `set_len` fails with os error 1224 (ERROR_USER_MAPPED_FILE); the pipeline finishes normally with all 1024 chunks. |

The SIGBUS kills ffsync before it commits anything, so the target is
untouched. A signal handler would be fragile and process-global; the real
fix is a pread-based pipeline, which reopens the mmap decision and must be
re-measured against `evaluation/chunking.md`. Decision for now: a known
limitation, documented in the README, the root AGENTS.md gotchas, and the
`SAFETY:` comment.

## Next: is a TLA+ model worth it?

| Candidate | Worth it? | Why |
|---|---|---|
| §16.8 membership + TREE_END barrier | **Done** (see above). | This is the protocol's only real concurrency. K sender workers race over K connections; the receiver replaces wire order with membership; and the `sender_end_consumed` gate exists because removing it deadlocks. `tree_session_converges` exercises K ≤ 3 over 24 runs, which explores only a handful of interleavings. A TLA+ spec with K = 2–3, a few files, and lossy or failing workers could check exactly-once membership, deadlock freedom, and "deletes only after every conversation succeeded" over all interleavings. Expect a small spec of about 150 lines. |
| §9.2 eager write gating | Yes, second (cheap). | This is a classic bounded-buffer deadlock question: two processes and two bounded channels, with the sender holding up to `EAGER_BUF_CAP` while the receiver streams its manifest. `eager_does_not_deadlock_on_small_transport_buffers` pins one configuration. A model would confirm that the gate "write after MANIFEST_DONE" is necessary and sufficient for every buffer size. Expect under 100 lines. |
| §17.6 QUIC teardown | No. | The hazard lives in quinn's and RFC 9000's close semantics (unacknowledged stream data is discarded), not in ffsync's state machine. A faithful model would mostly restate QUIC. A loss-injecting integration test around `wait_for_peer_close` is the better tool. |

Other follow-ups:

- **Run `cargo kani` once on Ubuntu 22.04+** (Ubuntu 20.04's glibc is too
  old for Kani 0.68) and record the timings and any bound adjustments
  here.
- ~~Add a scripted hostile sender to the property suite.~~ Done: see
  "Hostile-sender properties". It mutates the real sender's frames rather
  than scripting them, which keeps every case a plausible session.
- **Extend the hostile suite to a hostile receiver.** The sender side has
  the same exposure (plans, ACKs, TREE_END reports) and no property yet.
