# Parallel Session Latency Evaluation

Measured 2026-10-22 (re-verified against the v0.5 build), Windows,
release build of `bench` (`cargo build --release`). This evaluates §16.8
(`--parallel K`) — the D13 step-1 decision that parallel per-file
conversations are worth K TCP connections without QUIC or tokio.

## Command and fixture

```
cargo run --release --bin bench -- --latency --quick --out-dir bench_tmp/lat --seed 42 --parallel 1,4,8 --rtt-ms 30
```

Fixture: a generated tree of 100 files in 8 directories (~423 MiB;
fixture sizes are random, so the byte total varies by seed). The receiver
starts **empty** — every file is a fresh conversation, the worst case for
serialized round trips (deliberately chosen: comparable across K, and the
shape a first sync between peers takes).

## The wire model (`LatencyEnd`)

The real `tree_send`/`tree_receive` state machines run unchanged; only
the transport is simulated — an in-memory duplex where:

- every byte written appears at the peer after **one-way = RTT/2**,
- reads **coalesce** everything already queued before charging the
  one-way delay (a burst of frames written back-to-back costs one flight,
  like TCP batching),
- writes block when the bounded channel is full — the same backpressure a
  TCP socket buffer applies.

So the session wall time ≈ scan + I/O + Σ(conversation round trips × RTT),
and the harness measured exactly the thing the D13 amendment predicted:
the dead air between sequentially serialized conversations.

## Results (30 ms simulated RTT)

| parallel K | wall time | vs sequential |
|---|---|---|
| 1 (sequential, default) | 21.0 s | baseline |
| 4 | 6.25 s | 3.36× |
| 8 | 3.46 s | 6.07× |

First run (the one recorded in the D13 second amendment and the
CHANGELOG). The re-run executed for this document reproduced it within
run-to-run noise (output in `bench_tmp/lat_doc_run.txt`, git-ignored):

| parallel K | wall time | vs sequential |
|---|---|---|
| 1 | 21.28 s | baseline |
| 4 | 6.32 s | 3.37× |
| 8 | 3.46 s | 6.14× |

Spread ≈ 2% on the K=1/K=4 rows, identical K=8 wall time — the ratio is
stable, which is the quantity this evaluation is about.

## Reading the results

- **The K-scaling is the finding** — the mechanism (serialized round
  trips dominate at RTT ≫ LAN) and the shape of the curve, not the
  absolute seconds. K=8 gives 6.07×, not 8×: the sender's control
  connection always runs its share of conversations sequentially, a
  structural cap shared by any K-connection design (and the reason the
  curve must flatten as K → ∞).
- K=4 captures most of the win (3.36× of the 6.07× max); the CLI caps
  K at 16 (`MAX_PARALLEL`), and more connections mostly add handshake and
  scheduling cost past ~8 on realistic links.
- Sessions were re-run per K with a **fresh empty receiver** so every
  measurement is the full-transfer worst case and comparable across K.

## Real WAN (GitHub runner → quick tunnel → home uplink, 2026-10-23)

The shared-bottleneck test: a GitHub-hosted runner (Azure datacenter)
sends the seed-42 fixture (100 files, 423 MiB) through a Cloudflare
quick tunnel to a receiver on the home PC — the home **uplink** is the
scarce direction. Plaintext on both ffsync ends by design: the tunnel
is TLS end-to-end (D16 allows plaintext inside tunnels), and a one-shot
measurement skips the ephemeral-identity pin dance. Every completed
cell was hash-verified against the fixture on the receiver.

| cell | wall time | effective rate |
|---|---|---|
| K=1 (run A) | 61.14 s | ~55–58 Mbit/s |
| K=4 (run B) | 36.47 s | ~93 Mbit/s |
| K=8 (run B) | 41.34 s | ~82 Mbit/s |
| K=16 (run B) | 34.04 s | ~99 Mbit/s |

Findings, with the honesty the setup demands:

- **Parallel conversations help on the real path** — K=1 at 61 s vs
  ~34–41 s for K≥4. Suggestive ratio ~1.7–1.8×, but **cross-run**: the
  K=1 and K≥4 cells come from different runs with different tunnels
  (run B's K=1 cell failed — quick tunnels drop idle connections, and
  the runner's ~8 min build left the tunnel stale: `connection reset`
  before TLS). A within-run baseline (tiny pre-warm transfer per cell
  before the timed one) is needed before quoting a ratio.
- **Within run B, the curve is flat**: K=4/8/16 = 36.5/41.3/34.0 s —
  beyond 4 connections, more buy nothing on this path (the K=8 cell was
  *slower* than K=4; tunnel noise dominates). So even on a
  bottlenecked path, K=4 captures the available win.
- **The predicted "shared pipe collapses the K-ratio" did not happen** —
  parallel cells got *faster* than K=1. The likely reason: the path's
  bottleneck is not one fair-shared pipe. The cloudflared tunnel is
  partly per-connection-limited (single QUIC stream to the edge,
  userspace relay), so K flows bypass per-connection limits instead of
  sharing one fairly. On a clean, fair-bottleneck link (plain internet
  path, no tunnel) the collapse remains plausible — this experiment
  measures ffsync-over-tunnel, not ffsync-over-the-internet.
- Operational lesson for quick tunnels: they expire without traffic
  (~minutes), so any experiment must send *something* through each
  tunnel shortly before measuring it.

Not measured here: packet loss, jitter, a fair shared bottleneck (that
remains the `tc netem` case), and absolute user-experience numbers (the
uplink shape of one home connection is not a generalizable WAN).

## Real TCP check (toxiproxy, 2026-10-23)

The model's known blind spots — congestion window, real ACK clocking,
per-packet delay — are testable on the real kernel TCP stack without a
second machine: two actual `ffsync` processes (TLS, pinned fingerprints)
talk through a [toxiproxy](https://github.com/Shopify/toxiproxy) TCP
proxy on loopback, which delays each direction by RTT/2 (+5 ms jitter).
Same fixture (100 files, 423 MiB, 8 dirs, seed 42), receiver empty per
cell, every cell hash-verified for content convergence
(sender/receiver SHA-256 over all 100 files must match), sender exit 0.

| RTT | K=1 | K=4 | K=8 |
|---|---|---|---|
| 30 ms — model | 21.0 s | 6.25 s (3.36×) | 3.46 s (6.07×) |
| 30 ms — real TCP | 13.16 s | 5.05 s (2.61×) | 3.04 s (4.33×) |
| 150 ms — real TCP | 36.39 s | 15.18 s (2.40×) | 9.08 s (4.01×) |

Findings:

- **The mechanism survives real TCP.** The K-ratio is not an artifact of
  the channel model: K=4 buys ~2.4–2.6×, K=8 ~4.0–4.3× on a path with
  genuine RTT. Parallel conversations are worth their connections.
- **The model over-counts, as suspected.** Real K=1 (13.2 s) beats the
  model's 21.0 s at the same RTT: real TCP pipelines frames within a
  conversation better than the model's per-read latency charging, and
  toxiproxy delays bytes per direction crossing, letting in-flight data
  accumulate during the sleep instead of charging every read a full
  flight. The model is therefore a conservative upper bound on the dead
  air — the right tool for *whether* parallelism helps, not for *how
  much*.
- **Real ratios sit below the model's** (4.33× vs 6.07× at K=8/30 ms):
  same over-count mechanism — the sequential baseline loses more dead
  air in the model than it does on real TCP, so the model flatters the
  ratio too.
- 150 ms (intercontinental shape) shows the same ratios: the win scales
  with RTT in dead-air terms but the *relative* gain is stable across
  the two points measured.

Not simulated here either: toxiproxy applies its toxics per connection
direction, so K flows do **not** share one bottleneck pipe — the
shared-bandwidth congestion question (does the win shrink when the link,
not the RTT, is the bottleneck?) still needs `tc netem` with a single
`rate` queue (CI workflow or a VM). No packet loss was injected.

## What the model does not simulate

- **Bandwidth ceiling and congestion control**: no slow start, no cwnd,
  no packet loss. K connections saturating a real WAN link share it via
  TCP fairness — the 6.07× can shrink when the link, not the RTT, becomes
  the bottleneck. The model answers "how much of the wall time is dead
  air" (almost all of it); it does not answer "how fast is this on my
  link".
- **CPU contention**: sender/receiver threads share this machine with
  zero contention; real peers have load, and zstd spends real cycles.
- **One RTT point**: 30 ms is a plausible inter-region RTT. At LAN RTTs
  (< 1 ms) the whole regime vanishes (conversations are I/O-bound, and
  §16.8's `--parallel` buys little); at 150 ms+ intercontinental RTTs the
  dead air is worse and K=8 underestimates the win.
- **Jitter and reordering**: the model is deterministic; real WANs add
  variance (measured medians would differ from single runs).

## Absolute numbers are not UX numbers

21.0 s / 6.25 s / 3.46 s are the *model's* wall times, not what a user
experiences. The real-TCP section above is the closest local check (real
kernel TCP, real congestion window, real handshake — but loopback
bandwidth and no loss); the remaining gap to a user experience is a
genuine WAN with its bandwidth shape and loss profile. Before quoting
absolute seconds as user experience, measure two real peers across a WAN
(two cheap VPSes on different continents, `--parallel 1,4,8`, same
fixture) — the K-ratio on real TCP is the number that validates the
model. Until then, quote the ratio and the mechanism.
