# transport — TLS TOFU security + QUIC carrier

Transport security (D16) and the QUIC transport (§17, D13 step 2): rustls
TLS 1.3 with mutual TOFU fingerprint pinning, and a quinn-based QUIC carrier
riding the same identity/pins. TLS is a thin wrapper; QUIC adapts async quinn
to the sync protocol seam.

## STRUCTURE
- `mod.rs` — TLS security: `Identity` (44, self-signed cert + PKCS#8 key in
  `identity.bin`, created on first run), SHA-256 fingerprint codec
  (`AA:BB:…`), `PinPolicy` (164: `Pinned(set)` vs `FirstUse`),
  `server_session` (325) / `client_session` (347) — each completes the
  handshake eagerly and returns the stream + presented fingerprint;
  pins.conf load/add/remove (atomic rewrite).
- `quic.rs` — quinn carrier: `QuicStream` (82, sync `Read + Write + Send`
  adapter via block_on at the boundary), `QuicSession` (152, owns the one
  connection; worker conversations = extra streams), `wait_for_peer_close`
  (194), QUIC `client_session` (251) / `server_session` (306).

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Trust decision / pins | `mod.rs` pin functions | CLI applies TOFU, not this module |
| Handshake behavior | `client_session` / `server_session` | both files |
| QUIC streams / sessions | `quic.rs` | tokio confined here |
| State files | `state_dir()` | `identity.bin`, `pins.conf` |

## CONVENTIONS
- Tokio lives ONLY here (D3 edge exception). Everything this module returns
  is sync; quinn's `Endpoint::connect`/`Incoming::accept`/`Endpoint::new`
  need a tokio runtime context — route them through the module's `block_on`.
- The verifier never makes trust decisions by itself (`mod.rs:162`): the CLI
  applies the TOFU decision (prompt or `--trust`) AFTER the handshake, before
  any sync data.
- Pins are a set, not per-name entries; any pinned fingerprint may connect in
  either role. Worker connections authenticate with the SAME pin policy as
  the control connection (closures injected by the CLI).
- `--plaintext` bypasses TLS entirely (TCP only); QUIC shares identity/pins.

## ANTI-PATTERNS (THIS PROJECT)
- Never silently regenerate a corrupt `identity.bin` — refuse loudly. The key
  IS every peer's pin; regeneration breaks them all.
- QUIC teardown is NOT TCP teardown (§17.6 — see the root AGENTS.md QUIC
  gotcha): an answered final frame (TREE_END report, COMMIT_ACK) is only
  safe when the writer holds the connection until the reply/close is
  observed — `wait_for_peer_close` guards the receive paths and the tree
  sender; the single-file send path deliberately does not wait
  (`main.rs:695`).
- Dropping a `QuicStream` auto-finishes (FIN) — that is what the receiver's
  Eof path expects; a dropped `RecvStream` sends STOP_SENDING, so never rely
  on reading a stream after its `QuicStream` was dropped.
- `client_session` keeps its bounded handshake timeout: quinn retransmits
  Initials forever, and a dead peer must fail loudly (§17.5).
- Windows is the primary platform; identity file permissions: `0600` on unix,
  inherited user-profile ACLs on Windows.

## HOTSPOTS
- `mod.rs` (~550 lines) mixes TLS wiring, pin codec, and sessions; `quic.rs`
  is the only tokio user in the crate. Tests: `tests/tls.rs` (TCP loopback),
  `tests/quic.rs` (UDP loopback).
