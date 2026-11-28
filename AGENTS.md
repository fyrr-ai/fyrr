# Agent instructions

[CONTRIBUTING.md](CONTRIBUTING.md) is the contributor guide: build, layout,
verification and the rules every change follows (tests, contracts,
secrets, reader-only build, commits). Follow it; this file adds only what
agents must do on top of it.

## Protocol

1. Run `scripts/doctor` before the first work in a checkout.
2. Make the change. Code, comments, docs, commit messages and PR text are in
   English. Follow the [commit template](.gitmessage) and the
   [pull request template](.github/pull_request_template.md) when writing
   messages from the command line, too. To load the commit template in your
   Git editor, run `git config --local commit.template .gitmessage`.
3. Follow [Contributing: Verifying](CONTRIBUTING.md#verifying) for CI tiers,
   required checks and per-step summary reporting.

## Changes

- One pull request per change; a stacked change says which PR it builds on.
- Write the least code that works: reuse what the codebase already has,
  delete rather than add, and add no option, abstraction or "for later" code
  nobody asked for. Leave no dead code behind.
- fyrr is built on the Lance crates, pinned to one version in `Cargo.toml`.
  Before you build an engine feature, check whether that Lance version
  already provides it, and use it if so. If it does not, but a Lance change
  already merged upstream will, do not build it here: say so on the issue
  and stop. Bump the Lance pin only when the issue asks for it.
- Every object-store request is paid for. The storage budget tests in
  `crates/fyrr/tests/dense_path/storage_budget.rs` hold the reader and the
  idle writer to their request counts; raise a budget only on purpose, and
  never give an idle writer an object-store write.
- Measure on small namespaces unless the change is about scale.

## Before you open a pull request

Show these in the pull request; reviewers check them first.

- A new or changed test fails without the change: revert or mutate the fix
  and show the failing run.
- A change to a read or commit path reads no more than main: compare the
  object-store requests or reads per operation against main (the storage
  budget tests, or a counted run).
- Code that splits, matches or highlights text handles non-ASCII input and
  window or token boundaries: run it on such inputs.

## Issues

An issue is ready for an agent when it says what is wrong (with a number
where one exists), where in the code, when it is done (a checkable result),
and how to verify it (the `scripts/ci` row or test). General rules stay in
this file and CONTRIBUTING.md, not in issues.

## Safety

- Use rustup's Cargo (`~/.cargo/bin` first on `PATH`) with this checkout's
  own `target/`.
- Builds and test cases of every checkout share the mbx scheduler, and it
  divides the cores. Do not throttle `scripts/ci` with `--jobs`, `taskset`,
  `nice` or `CARGO_BUILD_JOBS` to make room for another checkout. Use one
  mbx store (`MBX_CACHE_DIR`) for every command in a checkout; a second one
  recompiles the whole dependency graph.
- Start and stop the fixtures only with `scripts/s3-fixture` and
  `scripts/keycloak-fixture`; build images only with `scripts/images`.
- Never prune Docker or touch containers, images or data you did not create.
