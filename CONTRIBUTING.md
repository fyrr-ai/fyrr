# Contributing

Thank you for helping. This guide covers the build host, the one
verification command and the rules every change follows. Security issues go
through [SECURITY.md](SECURITY.md), not public issues.

## Build host

Builds are supported on **Linux x86-64 and aarch64**: the pinned tools
(`scripts/pins.env`) are Linux binaries for those two architectures and the
scripts use Bash and GNU utilities. `scripts/ci` runs on x86-64; aarch64 is
built and its images are checked by the release job. Linux is the only
supported build host.

Prerequisites: git, curl, unzip, openssl, Python 3.11 or newer, Docker with the compose
plugin, rustup, uv,
mise with `mise install` (mr-boxington, pinned in `mise.toml`), binutils (`readelf`), jq, a C compiler (GCC 12+ or Clang 6+), and about 10 GiB
of free space for `target/`.

```sh
scripts/doctor           # all checks, including Docker
scripts/doctor --local   # local development without Docker
```

`scripts/doctor` checks the pinned Rust toolchain and components and prepares
checksum-verified protoc and cargo-deny in `target/tools`, uv-managed Python
and the locked `connectors/.venv`. Default mode also prepares the S3 fixture
image. Local mode warns about missing CI-only mbx. It is idempotent.

## Building

```sh
cargo build --locked -p fyrr                                   # full build
cargo build --locked -p fyrr --no-default-features             # reader-only build
```

Both builds write `target/debug/fyrr`, so the second replaces the first; use
`--target-dir` to keep both. Use rustup's Cargo (`~/.cargo/bin` first on
`PATH`) and this checkout's own `target/`. `.cargo/config.toml` points
`PROTOC` at the pinned protoc. The build links no Python.

To run the bundled providers from a development build, point the writer at
the venv: `VIRTUAL_ENV=$PWD/connectors/.venv target/debug/fyrr writer
providers` (the full tests set it themselves). The Python SDK has its own
tests: `cd connectors && uv sync --locked --all-extras && uv run pytest`. It
supports Python 3.11 or newer (tested on 3.11, 3.12, 3.13 and 3.14); to try
another interpreter without touching `connectors/.venv`, give `uv` its own
environment, for example
`UV_PROJECT_ENVIRONMENT=/tmp/venv311 uv run --python 3.11 --all-extras pytest`.

`cd connectors && uv run --locked --all-extras mypy` checks the SDK, bundled
providers and example provider. Bodies are checked with partial signature
annotations; PyArrow's untyped API remains covered by Arrow contract tests.

## Layout

| Path | Content |
| --- | --- |
| `crates/fyrr-core` | Contracts, configuration, MCP tool schemas. No S3 or Lance. |
| `crates/fyrr-reader` | Pinned Lance reads, authorization, hybrid search, read-through mirror, MCP over stdio and HTTP. |
| `crates/fyrr-writer` | Full build only: provider discovery, workers and their Python provider host, batch validation, committer, maintenance, directory and SCIM. |
| `crates/fyrr` | The `fyrr` binary; feature `writer` (default) is the full build. |
| `connectors/` | Python package `fyrr-connectors`: provider API, test kit and the bundled providers. |
| `clients/python/` | Python package `fyrr-client`: synchronous and asynchronous clients of the HTTP API, with types generated from `schemas/openapi.json`. Separate from the binary. |
| `clients/typescript/` | npm package `fyrr-client`: a client of the HTTP API for server runtimes, with types generated from `schemas/openapi.json`. Separate from the binary; tested by the `typescript-client` workflow, not by `scripts/ci`. |
| `examples/` | The quickstart (configuration, documents and `run`), an independently built provider wheel and a derived-image Dockerfile. |
| `schemas/`, `fixtures/` | Generated JSON Schemas, golden contract documents and MCP goldens. |
| `plugins/`, `.claude-plugin/` | The optional Claude Code plugin and its marketplace manifest. Raise `version` in `plugins/fyrr/.claude-plugin/plugin.json` with every change; clients cache a plugin by version. |
| `docker/` | `Dockerfile` (targets `fyrr-reader` and `fyrr`), entry point, `compose.yaml`. |
| `deploy/helm/fyrr` | Helm chart: readers and one writer. Rendered and linted by `scripts/images`. |
| `deny.toml` | License and advisory policy. |

## Verifying

```sh
scripts/ci --quick      # before a PR: core gates and local security scenarios
scripts/ci              # full suite: storage, connectors or maintenance changes
scripts/ci --images     # full suite plus image checks (compiles release binaries)
scripts/ci --jobs=2     # bound concurrent Rust cases (default: CPU/memory based)
scripts/ci --step=configuration "--step=schema drift" # exact names; others n/e
scripts/ci --step="reference docs" # standalone reference-doc verification
scripts/check-docs --render        # rewrite the generated docs: API routes, README index
```

`scripts/ci` runs doctor, rustfmt, clippy for the workspace and the
reader-only build, the test-case ownership guard, unit, contract,
configuration and schema tests in the full build, CLI and privacy tests in
both builds, the debug
reader-only link check, `cargo deny`, the Python SDK, the Python client and the example
provider, the quickstart on local storage, local-storage scenarios,
and a small external-services set against owned RustFS and Keycloak fixtures.
It keeps going after a failure and writes
`target/ci-logs/<UTC stamp>/` with one log per step and `summary.txt`.
Set `FYRR_CI_LOG_DIR` to choose a different directory. Relative paths are
resolved from the checkout root.

Build helpers use pinned mr-boxington (`mbx`) with rustup's Cargo first on
`PATH`. Each test feature set is built once; cases execute preserved full
and reader-only artifacts in a bounded pool. Set `--jobs=N` or
`FYRR_CI_JOBS=N` to bound concurrency. Every case also reserves two cores
from the mbx scheduler, which the builds of every checkout on the machine
share, so concurrent runs in several worktrees wait for cores instead of
slowing each other's cases past their deadlines. Do not lower `--jobs` to
make room for another checkout. Each case owns its fixtures; failed
fixtures and all logs remain for diagnosis (`FYRR_CI_KEEP_FIXTURES=1` also
keeps successful fixtures). When `/dev/shm` is a writable tmpfs, fixtures
are in `/dev/shm/fyrr-<hash of the checkout>` (`FYRR_TEST_TMP`); the next
top-level run removes them when no process of the checkout uses them, and a
reboot always. Never share a
writable `CARGO_TARGET_DIR` or run two top-level CI processes in one
checkout. Compiler actions can be shared across worktrees; their Cargo
outputs stay private. The shared store is kept under 40 GiB (`MBX_GC_MAX_SIZE`).

`scripts/ci-cases.json` owns the cases and their `quick`/`full` tiers; the
ownership gate checks all cases, including those excluded from the selected tier.
A row owns the cases under its prefixes (`owns`). A prefix ends at a test group
(`full:fyrr:test:dense_path/`) or a module (`full:fyrr:test:dense_path/incremental::`)
and carries a tier and `seconds`, the mean time of one case; the pool only uses
it to start long cases first. The longest prefix wins, and an exact entry in
`cases` beats any prefix. `children` names the tests that a parent case runs
in a child process. A row marked `batch` needs no fixture: one worker runs
its cases in one test process per test binary, with `TMPDIR` set to
`FYRR_TEST_TMP`, and reads each case's result from libtest's output into
`cases.json`.

Run `scripts/ci --quick` before a PR. It runs all core gates, CLI and privacy
cases, the Python SDK and example, local quickstart, and the local scenarios
for permission filtering, revocation, fail-closed authorization and CAS/leases.
Its `doctor --local` preflight does not require Docker or fetch service images.
The full `scripts/ci` is required when changing storage, connectors or
maintenance; it adds the remaining local scenarios, providers, real S3,
Keycloak and long maintenance/history campaigns. Cases excluded by the tier
are `n/e` in `case-plan.json` and row results; wholly excluded rows are `n/e`
in `summary.txt`. A mixed row counts only its selected cases as passed.

PRs and pushes to main run `--quick`. Changes confined to
`examples/quickstart/` run the local quickstart row when successful CI has
cached a binary for their exact base commit on main. A cache miss runs the
usual quick tier. The `v*` tags run the full tier and then build the release
artifacts and images in a separate job (see
[Releasing](#releasing)).
New PR commits cancel superseded CI runs. On main, the active run finishes
and the newest pending revision runs next.
Private repositories use `--debug-only` and skip release artifacts.

This checkout owns its `target/`. Fat-LTO and image checks run only with
`--images` and in `scripts/release`; quick and full CI check the debug
reader-only binary's Python independence and report release-only rows as
`n/e`.

Read `summary.txt` the way CI does:

- a test step that selected zero tests **fails**;
- a check that did not run is `n/e` (not evaluated), never passed;
  release-only rows are `n/e`;
- report the per-step passed, failed and ignored counts with your change.

Changes to `docker/`, the SDK's packaging, provider install paths or
anything that moves image size also need `scripts/ci --images`.

| Script | What it does |
| --- | --- |
| `scripts/ci-cases check\|inventory\|run ROW` | Test-case ownership; runs one CI row exactly as CI does. |
| `scripts/s3-fixture start\|env\|status\|stop` | A loopback RustFS container for this checkout; `env` prints the `FYRR_TEST_S3_*` variables. |
| `scripts/keycloak-fixture start\|env\|status\|stop` | A loopback Keycloak for the identity rows. |
| `scripts/check-reader-only` | Proves the reader-only build contains no writer or provider code, and that neither build links Python. |
| `scripts/bench-write [--storage local\|fixture\|s3]` | Measures write latency and write-to-searchable time of the real HTTP path: p50, p90 and p99 per batch size, object-store operations per write, and the fragment and index-segment counts. Results go in [docs/investigations/write-latency.md](docs/investigations/write-latency.md). |
| `scripts/bench-filtered-recall` | Measures the recall@10 of filtered vector search on the real HTTP path, for filters that keep 50%, 10%, 1% and 0.1% of the rows with values spread evenly and clustered by partition, against the exact scan. Results go in [docs/tuning.md](docs/tuning.md#dense-search-under-a-request-filter). |
| `scripts/bench-fts-positions` | Builds the full-text index of the same corpus without and with token positions on the real writer and measures the growth in bytes. Results go in [docs/tuning.md](docs/tuning.md#the-size-of-the-positions). |
| `scripts/images` | Builds, tests and measures both images. It compiles the binaries in the pinned builder stage, or packages existing ones with `--binaries DIR`. |
| `scripts/release` | Assembles versioned local release artifacts with SHA-256 sums; it publishes nothing itself (see [Releasing](#releasing)). |

`scripts/images` retains superseded image IDs. Reclaim their storage manually
only after confirming that no tag, digest or container still needs them.

Run an individual scenario through the same entry point:

```sh
scripts/ci "--step=local: tools"
scripts/ci "--step=real S3: object-store"
```

Local rows need no Docker fixture. `scripts/ci` supplies their development
secrets and child executable paths. `FYRR_TEST_BACKEND` selects local or S3
for shared scenario bodies; the committed ownership map selects it in CI.

The fixture scripts touch only the containers they labelled for this
checkout. Never prune Docker or remove containers, images or volumes you did
not create.

## Rules for changes

- **Tests.** Every executable test case has exactly one owner row in
  `scripts/ci-cases.json`. A new test under a prefix a row already owns needs
  no edit. A new module needs a prefix in the row that runs it; a test that
  must run in another row or tier than its prefix gives it needs an exact entry
  in that row's `cases`. A new test in a mixed module takes the row and tier of
  the module's prefix, so check where it runs. `scripts/ci-cases check` fails
  on an unowned case, a stale entry, or an exact entry its prefix already covers.
- **Contracts.** `schemas/`, the SDK's schema copies and `fixtures/mcp/` are
  generated from the Rust types. Regenerate them with
  `FYRR_UPDATE_GOLDEN=1 cargo test --locked -p fyrr-core --test generated`
  and review the diff. Compare each changed contract with its last released
  version. If all changes are additive (for example, new optional fields),
  use the next minor version; otherwise use the next major version, and
  published data is then republished, not migrated. A released 5.0 that
  became 5.1 for an additive change stays 5.1 for further additive changes
  and becomes 6.0 after a breaking one. The SDK's `versions.py` must match the
  contracts it ships. The configuration schema has its own
  `x-fyrr-schema-version`, currently `6.0` (S3 and local storage).
  `schemas/openapi.json`, the OpenAPI description of the HTTP API, is
  generated from the request and response types of the `/v1/` routes
  (`fyrr_core::http_api`, `fyrr_core::mcp`). A change of such a type also runs
  `scripts/ci "--step=schema drift"`; the Python client's types are generated
  from it, and `scripts/ci "--step=python client"` fails until you regenerate
  them (`cd clients/python && uv run python tools/generate_models.py`). The
  TypeScript client's types come from it too: the `typescript-client` workflow
  fails until you run `npm run generate` in `clients/typescript`.
- **Dependencies.** A new dependency needs a license and advisory verdict in
  `deny.toml`.
- **Secrets and privacy.** Secrets never enter the repository, configuration
  files or logs; configuration holds `{ env = "VAR" }` references. The
  privacy test (`crates/fyrr/tests/privacy.rs`) plants markers in secrets,
  document text, identities and queries, runs every CLI path and asserts that
  none appears in output or written files. A new CLI path, log or artifact
  joins that test.
- **Python.** No crate links Python. Providers run in a Python host process
  that a `fyrr writer __worker` process starts in its own process group
  (`python -S -c BOOTSTRAP`, built in `python/host.rs`) and talks to over a
  socket that is the host's stdin. The bootstrap uses only built-in modules to
  settle the descriptors (the socket moves to a close-on-exec descriptor and
  every other inherited descriptor is closed) before it runs site code and
  imports `fyrr_connectors.host`. The frames are bounded before they are read,
  Arrow streams are inspected before they are decoded, and every failure is a
  closed diagnostic. A change to the protocol bumps `PROTOCOL_VERSION` on both
  sides (`host.py` and `python/wire.rs`).
- **Reader-only build.** It contains no writer and no provider code;
  `scripts/check-reader-only` proves it.

## Code and commits

- `cargo fmt --all` and `cargo clippy … -D warnings` for both builds;
  `ruff check` and `ruff format` for Python.
- Code, comments, docs and commit messages are in English.
- Subjects follow `type(scope): summary` (`fix(writer): …`,
  `docs(connectors): …`), imperative, at most 72 characters. The body says
  what changed and why and, for behavior changes, which `summary.txt` rows
  verify it.
- One change per commit: a behavior fix and an unrelated refactor are
  separate commits.
- No chronology, machine names, local paths, credentials or ticket
  identifiers in code, docs or messages. Documentation describes current
  behavior.
- Do not commit `fyrr.toml`, `.env` files or build outputs.

## Releasing

`scripts/release` uploads nothing. Raise `version` and `appVersion` in
`deploy/helm/fyrr/Chart.yaml` with the workspace version (`scripts/images`
fails when they differ). Pushing a `v*` tag that matches the
workspace version in `Cargo.toml` starts `.github/workflows/release.yml`: it
runs CI (`scripts/ci`, full tier) and, only if CI passes, runs
`scripts/release` once per platform (linux-x86_64 and linux-aarch64). Each
of those jobs links its fat-LTO binaries once; its images package those same
files (`image_binaries release` in the platform's manifest) and nothing else
in the pipeline compiles a release binary. `scripts/release-combine` joins
the platforms: `manifest.txt` lists them, each platform's own manifest is
kept as `manifest-<platform>.txt`, and `SHA256SUMS` covers every file. The
workflow uploads that directory to a draft GitHub release, pushes each image
to GHCR per architecture and as one multi-arch manifest under the version,
and then publishes the release; reruns resume the draft or leave an
already published release unchanged. Release logs stay under
`target/release-logs/<version>/`, outside the downloadable artifacts. Image
archives retain checkout-owned tags, recorded as `image_tag` in the
platform's manifest. Bare binaries use the host ABI; their measured runtime
requirements are recorded in the same manifest (see
[downloadable binaries](docs/deployment.md#downloadable-binaries)).

The build jobs need more memory than GitHub's free runners have: a fat-LTO
link peaks above 12 GB. They run on Blacksmith runners by default
(`blacksmith-16vcpu-ubuntu-2404` and `blacksmith-16vcpu-ubuntu-2404-arm`),
which requires the Blacksmith GitHub App on the repository. To use other
runners, set the repository variables `RELEASE_RUNNER_X86_64` and
`RELEASE_RUNNER_AARCH64` to labels with at least 24 GB of memory. Running
the workflow by hand (`workflow_dispatch`) builds and joins the artifacts
without publishing anything. Without suitable runners, assemble the release
on a maintainer machine as described below.

A local `scripts/release` on a host whose glibc is newer than the runtime
images' cannot package its binaries: the images then compile their own in
the builder stage (`image_binaries builder`), a second fat-LTO build.
To release from such a host with one build, compile in the builder stage
(`docker build --file docker/Dockerfile --target builder`), copy `/out/fyrr`
and `/out/fyrr-reader` out of that image and pass their directory as
`scripts/release --binaries DIR`; the manifest then records
`binary_build supplied`.
After downloading the artifacts, verify and load the full image with:

```sh
sha256sum --check SHA256SUMS
image_tag="$(awk '$1 == "image_tag" {print $2}' manifest-linux-x86_64.txt)"
docker image load --input fyrr-image-0.1.1-linux-x86_64.tar.gz
docker run --rm "fyrr:$image_tag" --version
```

The reader archive uses the same tag under `fyrr-reader`.
When tagging, rename `[Unreleased]` in `CHANGELOG.md` to `[X.Y.Z] - YYYY-MM-DD`.
