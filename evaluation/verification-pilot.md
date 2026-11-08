# Verification pilot: property tests and bounded proofs (2026-11-08)

This pilot asked whether property testing (proptest) and bounded model
checking (Kani) find anything the example-based suites, the fuzzers and two
bug hunts missed, and which TLA+ model would be worth building next.

**Result in short:**

- All 27 properties pass on Windows. None fails on current code. They have
  not run on Linux yet: the local WSL distro has no C toolchain (`cc` is
  missing; `rustls`, `zstd-safe` and others need one), so CI's Linux job
  will be the first Linux run.
- Planted bugs make them fail, so they have teeth. Mutation check: a session
  expectation that ignores `--no-delete`, an off-by-one in the reference
  glob's class ranges, and a halved mtime tolerance each produced a failing
  property with a shrunk counterexample.
- Writing the properties surfaced one real bug next to them: a hostile tree
  sender could crash the receiver with an out-of-range mtime. It is fixed in
  its own commit, with a regression test.
- Three spec and API observations are recorded below; none of them is fixed.
- The Kani harnesses type-check but have **not been run**. Kani is not
  installed here, and installing it was out of scope.

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
before the crate builds at all. It is not in CI.

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

Status: written and type-checked; **not run**. The first `cargo kani` run
in WSL will show whether the unwind bounds are enough and how long CBMC
takes on the `format!` paths in `validate_rel_path`'s error branch.

## Findings

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

### Observation 3: `manifest_from_wire` does not check prefix consistency

`src/protocol/tree_sync.rs:180` accepts a manifest that lists both a file
`a` and an entry `a/b`. A scan can never produce this, so only a broken or
hostile sender would send it.

On such a manifest the receiver writes `a`, then fails loudly when it
reaches `a/b`, because the parent is a file. Nothing escapes the root and
nothing is silently lost, but files are written before the session fails.

Refusing the manifest up front, as the duplicate-path check already does,
would be cleaner. It is not fixed here because the current behavior is
fail-closed and changing it is a design choice. Candidate: one
ancestor-is-a-file check per entry over the `BTreeMap`.

## Next: is a TLA+ model worth it?

| Candidate | Worth it? | Why |
|---|---|---|
| §16.8 membership + TREE_END barrier | **Yes, first.** | This is the protocol's only real concurrency. K sender workers race over K connections; the receiver replaces wire order with membership; and the `sender_end_consumed` gate exists because removing it deadlocks. `tree_session_converges` exercises K ≤ 3 over 24 runs, which explores only a handful of interleavings. A TLA+ spec with K = 2–3, a few files, and lossy or failing workers could check exactly-once membership, deadlock freedom, and "deletes only after every conversation succeeded" over all interleavings. Expect a small spec of about 150 lines. |
| §9.2 eager write gating | Yes, second (cheap). | This is a classic bounded-buffer deadlock question: two processes and two bounded channels, with the sender holding up to `EAGER_BUF_CAP` while the receiver streams its manifest. `eager_does_not_deadlock_on_small_transport_buffers` pins one configuration. A model would confirm that the gate "write after MANIFEST_DONE" is necessary and sufficient for every buffer size. Expect under 100 lines. |
| §17.6 QUIC teardown | No. | The hazard lives in quinn's and RFC 9000's close semantics (unacknowledged stream data is discarded), not in ffsync's state machine. A faithful model would mostly restate QUIC. A loss-injecting integration test around `wait_for_peer_close` is the better tool. |

Other follow-ups:

- **Run `cargo kani` once in WSL** and record the timings and any bound
  adjustments here.
- **Add a scripted hostile sender to the property suite** (hand-written
  frames over a duplex stream). Session-level properties could then cover
  out-of-range wire values the way the finding above needed.
