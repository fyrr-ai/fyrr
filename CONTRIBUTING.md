# Contributing

Thank you for helping. This guide covers the build host, the one
verification command and the rules every change follows. Security issues go
through [SECURITY.md](SECURITY.md), not public issues.

## Build host

Builds and tests are supported on **Linux x86-64**: the pinned tools
(`scripts/pins.env`) are Linux x86-64 binaries and the scripts use Bash and
GNU utilities. Other platforms are not tested.

Prerequisites: git, curl, unzip, openssl, Python 3, Docker with the compose
plugin, rustup, uv,
cargo-deny 0.20.2, mise with `mise install` (mr-boxington 1.30.0), binutils (`readelf`), jq, a C compiler, and about 10 GiB
of free space for `target/`.

```sh
scripts/doctor           # all checks, including Docker
scripts/doctor --local   # local development without Docker
```

`scripts/doctor` checks all of that and prepares the pinned tools: the Rust
toolchain from `rust-toolchain.toml`, a checksum-verified protoc in
`target/tools`, the uv-managed Python and the locked `connectors/.venv` that
the full build links against, and the S3 fixture image. It is idempotent.

## Building

```sh
cargo build --locked -p fyrr                                   # full build
cargo build --locked -p fyrr --no-default-features             # reader-only build
```

Both builds write `target/debug/fyrr`, so the second replaces the first; use
`--target-dir` to keep both. Use rustup's Cargo (`~/.cargo/bin` first on
`PATH`) and this checkout's own `target/`. `.cargo/config.toml` points
`PROTOC` at the pinned protoc and `PYO3_PYTHON` at the connectors venv.

To run the bundled providers from a development build, point the writer at
the venv: `VIRTUAL_ENV=$PWD/connectors/.venv target/debug/fyrr writer
providers`. The Python SDK has its own tests:
`cd connectors && uv sync --locked --all-extras && uv run pytest`.

## Layout

| Path | Content |
| --- | --- |
| `crates/fyrr-core` | Contracts, configuration, MCP tool schemas. No S3 or Lance. |
| `crates/fyrr-reader` | Pinned Lance reads, authorization, hybrid search, read-through mirror, MCP over stdio and HTTP. |
| `crates/fyrr-writer` | Full build only: provider discovery, Python workers, batch validation, committer, maintenance, directory and SCIM. |
| `crates/fyrr` | The `fyrr` binary; feature `writer` (default) is the full build. |
| `connectors/` | Python package `fyrr-connectors`: provider API, test kit and the bundled providers. |
| `eval/` | `fyrr-eval`, an internal quality-measurement tool; never released. |
| `examples/` | The quickstart (configuration, documents and `run`), an independently built provider wheel and a derived-image Dockerfile. |
| `schemas/`, `fixtures/` | Generated JSON Schemas, golden contract documents and MCP goldens. |
| `docker/` | `Dockerfile` (targets `fyrr-reader` and `fyrr`), entry point, `compose.yaml`. |
| `budgets.toml`, `deny.toml` | Size budgets; license and advisory policy. |

## Verifying

```sh
scripts/ci              # every check
scripts/ci --jobs=2     # bound concurrent real-S3 rows (default: CPU/memory based)
scripts/ci --step=configuration "--step=schema drift" # exact names; others n/e
```

`scripts/ci` runs doctor, rustfmt, clippy for the workspace and the
reader-only build, the test-case ownership guard, unit, contract,
configuration, schema, CLI and privacy tests in both builds, the `fyrr-eval`
tests, the debug reader-only link check, `cargo deny`, the Python SDK and example
provider, the README quickstart on S3 and on local storage
(`examples/quickstart/run [--s3]`), the local-storage row and the real-S3
rows against local fixtures. It keeps going after a failure and writes
`target/ci-logs/<UTC stamp>/` with one log per step and `summary.txt`.

Compilation runs through the pinned mr-boxington (`mbx`) shared action
store. Shell build helpers call `mbx` with rustup's Cargo first on `PATH`;
`ci-cases` uses the same route. Plain Cargo is used for graph/metadata,
formatting and advisory commands that do not compile. Each test feature set
is built once; the ownership inventory and exact row selectors execute
those artifacts directly. The full and reader CLI binaries are preserved
separately and restored to Cargo's embedded executable path before each row.

Real-S3 rows run in a bounded pool, longest estimated rows first. Each row
has a private RustFS fixture and data directory; only the identity row starts
Keycloak. The default is at most six rows, bounded by CPUs, available memory
and configured swap. `--jobs=N` or `FYRR_CI_JOBS=N` overrides it. Rows are
serial within a fixture. Keep one CI process per checkout; separate
worktrees share compiler results without sharing writable Cargo outputs.

The same complete suite runs locally, on PRs, main, nightly and manual
workflow runs. There is no separate fast gate. GitHub uses the pinned
`jdx/mr-boxington-action` with its GitHub cache backend. Default-branch pushes
save; PRs restore only. Trusted manual runs may also save, allowing cache
validation on a branch. Target snapshots and the action store are distinct:
local worktrees share the action store under `~/.cache/mbx` (or
`MBX_CACHE_DIR`), while the action restores a pruned target snapshot on
hosted runners.

Repository scripts disable managed target relocation: this checkout owns
its `target/`, including fixtures and logs. This also keeps another build's
cache collection from evicting executables during direct test execution.
The shared action store defaults to a 10 GiB budget and 12 GiB minimum free
space (`MBX_GC_MAX_SIZE`, `MBX_GC_MIN_FREE_SIZE` override these). For other
projects, mbx's managed targets can also collect old worktree outputs;
this repository retains its own target directories. Never share a writable
`CARGO_TARGET_DIR` between worktrees.

Fat-LTO release links, release artifact feature-isolation checks, binary
budgets and image tests run in `scripts/release` on tags. CI retains the
normal dependency-graph checks and proves that its debug reader-only
binary has no Python dependency, with the full debug binary as a positive
control. CI reports the release-only rows as `n/e`.

Read `summary.txt` the way CI does:

- a test step that selected zero tests **fails**;
- a check that did not run is `n/e` (not evaluated), never passed;
  release-only rows are `n/e`;
- report the per-step passed, failed and ignored counts with your change.

Changes to `docker/`, the SDK's packaging, provider install paths or
anything that moves image size need `scripts/images` in addition to CI.

| Script | What it does |
| --- | --- |
| `scripts/ci-cases check\|inventory\|run ROW` | Test-case ownership; runs one CI row exactly as CI does. |
| `scripts/s3-fixture start\|env\|status\|stop` | A loopback RustFS container for this checkout; `env` prints the `FYRR_TEST_S3_*` variables. |
| `scripts/keycloak-fixture start\|env\|status\|stop` | A loopback Keycloak for the identity rows. |
| `scripts/check-reader-only` | Proves the reader-only build links no writer, Python or PyO3. |
| `scripts/check-budgets [--record]` | Dependency counts and release binary sizes against `budgets.toml`. |
| `scripts/images [--record]` | Builds, tests and measures both images. |
| `scripts/release` | Verifies release isolation and budgets, then assembles versioned local release artifacts with SHA-256 sums; it publishes nothing itself (see [Releasing](#releasing)). |

Run a single real-S3 row like this:

```sh
scripts/s3-fixture start && eval "$(scripts/s3-fixture env)"
scripts/ci-cases run s3-tools
scripts/s3-fixture stop
```

The `local-storage` row needs no fixture, only the development secrets that
`local_storage` in `scripts/ci` exports (`FYRR_TEST_LOCAL_SECRET`,
`FYRR_TEST_ENCRYPTION_KEY`, `FYRR_ROUTING_TEST_BINARY`).

The fixture scripts touch only the containers they labelled for this
checkout. Never prune Docker or remove containers, images or volumes you did
not create.

## Rules for changes

- **Tests.** Every executable test case has exactly one owner row in
  `scripts/ci-cases.json`. When you add, rename or remove a test, update its
  row in the same change; `scripts/ci-cases check` fails otherwise.
- **Contracts.** `schemas/`, the SDK's schema copies and `fixtures/mcp/` are
  generated from the Rust types. Regenerate them with
  `FYRR_UPDATE_GOLDEN=1 cargo test --locked -p fyrr-core --test generated`
  and review the diff. An additive change (a new optional field) bumps the
  minor version; anything else bumps the major, and published data is then
  republished, not migrated. The SDK's `versions.py` must match the
  contracts it ships. The configuration schema has its own
  `x-fyrr-schema-version`, currently `2.0` (S3 and local storage).
- **Budgets.** Dependency-count, binary-size or image-size growth needs
  `budgets.toml` updated in the same change (`--record`), by the measured
  delta only, explained in the commit message. A new dependency needs a
  license and advisory verdict in `deny.toml`.
- **Secrets and privacy.** Secrets never enter the repository, configuration
  files or logs; configuration holds `{ env = "VAR" }` references. The
  privacy test (`crates/fyrr/tests/privacy.rs`) plants markers in secrets,
  document text, identities and queries, runs every CLI path and asserts that
  none appears in output or written files. A new CLI path, log or artifact
  joins that test.
- **Reader-only build.** All Python runs in `fyrr writer __worker`
  processes; the reader-only build links no Python.

## Code and commits

- `cargo fmt --all` and `cargo clippy … -D warnings` for both builds;
  `ruff check` and `ruff format` for Python.
- Code, comments, docs and commit messages are in English.
- Subjects follow `type(scope): summary` (`fix(writer): …`,
  `docs(connectors): …`), imperative, at most 72 characters. The body says
  what changed and why and, for behavior changes, which `summary.txt` rows
  verify it.
- One change per commit: a behavior fix, a refactor and a budget update are
  separate commits.
- No chronology, machine names, local paths, credentials or ticket
  identifiers in code, docs or messages. Documentation describes current
  behavior.
- Do not commit `fyrr.toml`, `.env` files or build outputs.

## Releasing

`scripts/release` uploads nothing. Pushing a `v*` tag that matches the
workspace version in `Cargo.toml` starts `.github/workflows/release.yml`: it
runs CI (`scripts/ci`) and, only if CI passes, publishes the output
of `scripts/release` as a GitHub release and pushes both images to GHCR.
When tagging, rename `[Unreleased]` in `CHANGELOG.md` to `[X.Y.Z] - YYYY-MM-DD`.
