# TLA+ models

`ParallelSession.tla` models the conversation phase of a parallel tree
session (Protocol.md §16.8) and its TREE_END barrier (§16.5), frame by
frame. It covers K connections, the sender's shared work queue and
`stop` flag, the receiver's membership set, `cancel`, the worker joins,
the session decision, and the delete pass. Faults are connection resets,
a sender worker that dies without raising `stop`, and a local failure
inside a conversation on either side. With `Hostile = TRUE`, an arbitrary
sender replaces the reference sender: it writes any frames on any
connection and closes connections as it likes.

The results and what they mean are in `evaluation/verification-pilot.md`
("TLA+ model of the parallel-session barrier").

## Running

You need Java 11 or newer and `tla2tools.jar`. The results were recorded
with v1.7.4 from <https://github.com/tlaplus/tlaplus/releases>. Keep the
jar outside the repo. TLC writes a `states/` directory, so point
`-metadir` outside the repo too.

```bash
java -cp tla2tools.jar tlc2.TLC -workers auto -metadir <tmp> -config Honest.cfg ParallelSession.tla
```

| Config | Sender | Change | Expected result |
|---|---|---|---|
| `Honest.cfg` | reference, K = 3, 3 files, 1 fault | none | no error |
| `Hostile.cfg` | arbitrary, K = 2, 2 files + a stranger, 5 frames | none | no error |
| `Witness.cfg` (with `-continue`) | reference | none | `NoSuccess`, `NoQuietFailure` and `NoSplitOutcome` all violated: the model reaches each of those outcomes |
| `NoBarrierGate.cfg` | reference | the §16.5 barrier read always runs | `Termination` violated: both peers wait forever |
| `NoJoin.cfg` | arbitrary | the receiver decides before joining its workers | `DeleteSafe` violated |
| `NoMembership.cfg` | arbitrary | TREE_FILE is not checked against the set | `NoStranger` / `ExactlyOnce` violated |
| `NoRemaining.cfg` | arbitrary | success does not need an empty set | `DeleteSafe` violated |

Each config finishes in seconds, except `NoMembership.cfg` with
`-continue`, which takes about 3 minutes. A guard change in
`tree_sync.rs` means the matching constant and action here must change
too, and every config should be re-run.
