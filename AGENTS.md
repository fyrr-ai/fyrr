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

## Safety

- Use rustup's Cargo (`~/.cargo/bin` first on `PATH`) with this checkout's
  own `target/`.
- Start and stop the fixtures only with `scripts/s3-fixture` and
  `scripts/keycloak-fixture`; build images only with `scripts/images`.
- Never prune Docker or touch containers, images or data you did not create.
