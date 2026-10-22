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
experiences. The model cannot produce a transfer-time projection the way
`bench --size-mib` projects link speeds for the chunk pipeline, because
the wall time here is RTT-dominated by construction. Before quoting
absolute seconds as user experience, measure two real peers across a WAN
(two cheap VPSes on different continents, `--parallel 1,4,8`, same
fixture) — the K-ratio on real TCP is the number that validates the
model. Until then, quote the ratio and the mechanism.
