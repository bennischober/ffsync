# Contributing to ffsync

Thanks for considering a contribution. ffsync is a focused tool with a
strong design culture; this document is the short path into it.

## The big rules

1. **The spec is the contract.** `concept/Protocol v0.1.md` defines the
   wire protocol. Implementation deviations from it are bugs — if the spec
   is wrong, amend the spec FIRST (in the same PR), never the code alone.
2. **Decisions are recorded.** Substantive design choices go into
   `concept/Design Decisions.md` (the D-ledger) with reasoning and
   rejected alternatives — including decisions to NOT do something (see
   D7 flaw 2: implemented, measured, rejected, recorded).
3. **Claims are measured.** Performance statements need numbers from the
   bench harness with paired, seeded runs (`--seed 42`), recorded in
   `evaluation/` or the ledger. "Should be faster" is not a change
   rationale.
4. **The platform split is load-bearing.** Windows is the primary platform;
   platform-specific I/O lives behind `FileExt`
   (`src/reconstruction/io_ext.rs`). tokio exists only inside
   `src/transport/quic.rs` (D3); the protocol and data plane are native
   threads + crossbeam.

## Development

- Toolchain: current stable Rust; MSRV 1.88 is enforced by CI
  (`rust-version` in Cargo.toml).
- Gates (all must pass; CI enforces them on Windows AND Linux):

      cargo fmt --all -- --check
      cargo clippy --all-targets -- -D warnings
      cargo test
      cargo audit          # when dependencies changed

- The leak-guard soak test runs separately (100 sessions under a counting
  allocator):

      cargo test --test soak -- --ignored

- There are no rustfmt.toml / clippy.toml / toolchain pin files: default
  rustfmt and clippy behavior is authoritative.
- Run `cargo check --tests` first to establish a compile baseline if the
  tree may be mid-refactor.
- WSL note: if the checkout lives on a Windows mount (`/mnt/c/...`),
  build and test through the Windows toolchain —
  `cmd.exe /c "cd /d C:\path\to\ffsync && cargo ..."` — the Linux side
  cannot write to `target/` there. `bench_tmp/linux_ci.ps1` runs the CI
  gates in a container if you prefer.

## Sending changes

- Branches + pull requests to `main`; keep subjects in Conventional
  Commits form (`feat(tree): ...`, `fix(protocol): ...`).
- The PR template lists the per-change checklist (spec amendments, ledger
  entries, bench evidence).
- CI must be green before merge.
- For protocol-visible changes, state the old-peer behavior explicitly:
  every flag-gated feature so far fails LOUDLY on old peers (strict
  decode) — keep it that way.

## Where to start

- The 1.x roadmap lives in the D-ledger's open items: D8 (eager diff,
  fully designed in spec §9.2, needs a bench cell before implementation)
  and D7 flaw 2 (rejected with a documented retry barrier — read the
  amendment before attempting it).
- Docs and tests are as welcome as code; the knowledge base is the root
  `AGENTS.md` plus the per-directory AGENTS.md files.

## Reporting bugs and security issues

- Bugs: use the bug report issue template — environment details matter
  for a sync tool (both peers' versions, platforms, transport).
- Security: see [SECURITY.md](SECURITY.md) — please use private reporting,
  not public issues.
