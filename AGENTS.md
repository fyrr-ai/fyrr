# Agent instructions

[CONTRIBUTING.md](CONTRIBUTING.md) is the contributor guide: build, layout,
verification and the rules every change follows (tests, contracts, budgets,
secrets, reader-only build, commits). Follow it; this file adds only what
agents must do on top of it.

## Protocol

1. Run `scripts/doctor` before the first work in a checkout.
2. Make the change. Code, comments, docs, commit messages and PR text are in
   English.
3. Run `scripts/ci` and report its `summary.txt` (under `target/ci-logs/`)
   with the per-step test counts. A test step with zero selected tests
   fails; a check you did not run is `not evaluated` (`n/e`), never passed.
   Changes to `docker/`, the SDK's packaging, provider install paths or
   anything that moves image size need `scripts/ci --images`.

## Safety

- Use rustup's Cargo (`~/.cargo/bin` first on `PATH`) with this checkout's
  own `target/`.
- Start and stop the fixtures only with `scripts/s3-fixture` and
  `scripts/keycloak-fixture`; build images only with `scripts/images`.
- Never prune Docker or touch containers, images or data you did not create.
