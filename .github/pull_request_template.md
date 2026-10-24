## What

<!-- One paragraph: the change and its motivation; link the issue or the
     D-decision it implements. -->

## Verification

<!-- How you proved it: the exact commands and results (cargo fmt/clippy/
     test locally or CI), and bench numbers for performance claims
     (seeded, paired before/after with --seed 42). -->

## Checklist

- [ ] Conventional Commits subject: `type(scope): summary`
- [ ] Gates green locally: `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`
- [ ] Wire-protocol change? The spec was amended FIRST (same PR), `VERSION` considered, and old-peer behavior is loud (strict decode)
- [ ] New design decision? Recorded in `concept/Design Decisions.md` with reasoning
- [ ] Performance claim? Measured with seeded paired runs and recorded in `evaluation/` or the ledger
- [ ] Platform split respected: platform I/O behind `FileExt`; no tokio outside `src/transport/quic.rs` (D3)
- [ ] Docs updated where behavior changed (README, AGENTS.md, spec)
