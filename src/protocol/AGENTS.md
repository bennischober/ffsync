# protocol — wire protocol and session state machines

Protocol v4 (`MAGIC` + `VERSION` in `mod.rs`). Length-prefixed framing, typed
message codecs, and the sender/receiver state machines for single-file sync
(v2 flow) and tree sessions (v3 sequential, v4 parallel). Spec:
`concept/Protocol.md` — implementation deviations are protocol bugs
unless the spec is amended first.

## STRUCTURE
- `framing.rs` — `Frame` (56) / `read_frame` (125), `T_*` frame-type
  constants, `MAX_PAYLOAD` guard.
- `message.rs` — HELLO/manifest/plan/DATA/VERIFY codecs; `Message` trait
  (161); `Role`, `ErrorCode`; `FLAG_*` / `TREE_FLAG_*` bitflags;
  `TreeBegin` / `TreeWorker`.
- `sync.rs` — single-file state machines: `send` (270) / `receive` (590);
  exact-hash diffing (D12), zstd delta transfer, locals-first write order,
  mtime/size fast path (sub-second), resume (D10), warm-start integration
  (D15).
- `tree_sync.rs` — `tree_send` (306) / `tree_receive` (629):
  TREE_BEGIN/TREE_END envelope, sequential per-file conversations, deferred
  deletions; v4 parallel workers (`MAX_PARALLEL` 44), membership validation
  via TREE_FILE.
- `mod.rs` — re-exports plus MAGIC/VERSION.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Add/extend a frame type | `framing.rs` constant + `message.rs` codec | keep codecs strict |
| Single-file flow change | `sync.rs` | |
| Tree / parallel flow change | `tree_sync.rs` | |
| Protocol version bump | `mod.rs` `VERSION` + spec doc | amend spec first |

## CONVENTIONS
- State machines are generic over `S: Read + Write` — transport-agnostic.
  Never reference TCP/TLS/QUIC specifics here; the CLI injects connect/accept
  closures.
- Wire naming: `T_*` frame types, `FLAG_*` / `TREE_FLAG_*` bitflags, magics
  as `*b"..."` byte strings.
- Unknown flag bits fail loudly (`message.rs:1001`) so flag-gated fields can
  never silently change what a peer parses.

## ANTI-PATTERNS (THIS PROJECT)
- `ffsync::protocol` MUST NOT depend on tokio (D3). Async lives only in
  `src/transport/quic.rs`; the seam here stays sync.
- Tree NOT_MODIFIED path: do NOT drain or close (`sync.rs:382`) — a tree
  session continues after a conversation; a single-file session ends after
  one. The two flows look similar and must NOT be unified.
- Never truncate on resume — the temp file content IS the resume state
  (D10, `sync.rs:923`).
- Tree triage must be deterministic and identical on both peers; the receiver
  derives its flags from the sender's TREE_BEGIN and scans its tree only
  AFTER the handshake (CDC parameters travel authoritative in HELLO —
  scanning earlier desyncs chunk layouts).
- v4 parallel sessions replace wire ORDER with membership: every conversation
  (control connection included) is announced by TREE_FILE and validated
  exactly once. The receiver's control loop consumes the sender's TREE_END
  itself (gated on `sender_end_consumed`) — removing that gate deadlocks the
  sender on its TREE_END reply.
- Excluded paths are never deleted (`tree_sync.rs:1277`); the receiver's own
  exclude patterns only guard its delete pass, never manifests.
- Chunk payload data crosses the wire only in DATA frames — never through
  manifests or internal channels (`sync.rs:1029`, spec §12 resource limits).

## HOTSPOTS
- `sync.rs` (~1390 lines) and `tree_sync.rs` (~1336) are the largest state
  machines; `message.rs` (~1016) is codec-dense. Unit tests are inline;
  integration coverage lives in `tests/{loopback,tree,quic}.rs`.
