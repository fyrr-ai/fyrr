# Agent instructions

[CONTRIBUTING.md](CONTRIBUTING.md) is the contributor guide: build, layout,
verification and the rules every change follows (tests, contracts,
secrets, reader-only build, commits). Follow it; this file adds only what
agents must do on top of it.

## Protocol

1. Run `scripts/doctor` before the first work in a checkout.
2. Make the change. Code, comments, docs, commit messages and PR text are in
   English.
3. Follow [Contributing: Verifying](CONTRIBUTING.md#verifying) for CI tiers,
   required checks and per-step summary reporting.

## Changes

- One pull request per change; a stacked change says which PR it builds on.
- Write the least code that works: reuse what the codebase already has,
  delete rather than add, and add no option, abstraction or "for later" code
  nobody asked for. Leave no dead code behind.
- Every object-store request is paid for. The storage budget tests in
  `crates/fyrr/tests/dense_path/storage_budget.rs` hold the reader and the
  idle writer to their request counts; raise a budget only on purpose, and
  never give an idle writer an object-store write.
- Measure on small namespaces unless the change is about scale.

## Issues

An issue is ready for an agent when it says what is wrong (with a number
where one exists), where in the code, when it is done (a checkable result),
and how to verify it (the `scripts/ci` row or test). General rules stay in
this file and CONTRIBUTING.md, not in issues.

## Safety

- Use rustup's Cargo (`~/.cargo/bin` first on `PATH`) with this checkout's
  own `target/`.
- Start and stop the fixtures only with `scripts/s3-fixture` and
  `scripts/keycloak-fixture`; build images only with `scripts/images`.
- Never prune Docker or touch containers, images or data you did not create.
