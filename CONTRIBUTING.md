# Contributing

Thank you for helping. This guide covers the build host, the one
verification command and the rules every change follows. Security issues go
through [SECURITY.md](SECURITY.md), not public issues.

## Build host

Builds are supported on **Linux x86-64 and aarch64**: the pinned tools
(`scripts/pins.env`) are Linux binaries for those two architectures and the
scripts use Bash and GNU utilities. `scripts/ci` runs on x86-64; aarch64 is
built and its images are checked by the release job.

**macOS on Apple Silicon** is a development host with weaker worker
isolation ([details](docs/deployment.md#macos-development-only)). The
scripts need Homebrew's Bash and GNU coreutils ahead of the system's:
`brew install bash coreutils jq` and put `$(brew --prefix)/bin` and
`$(brew --prefix)/opt/coreutils/libexec/gnubin` first on `PATH`. Builds use
plain Cargo there. Run `scripts/doctor --local`, then the subset CI runs on
macOS:

```sh
scripts/ci --quick "--step=unit tests" "--step=configuration" \
  "--step=cli (full build)" "--step=cli (reader-only build)" "--step=local storage"
```

Docker fixtures, images and the remaining rows are Linux-only.
`scripts/release-macos` assembles the macOS archives. Windows is not
supported.

Prerequisites: git, curl, unzip, openssl, Python 3.11 or newer, Docker with the compose
plugin, rustup, uv,
cargo-deny 0.20.2, mise with `mise install` (mr-boxington 1.30.0), binutils (`readelf`), jq, a C compiler, and about 10 GiB
of free space for `target/`.

```sh
scripts/doctor           # all checks, including Docker
scripts/doctor --local   # local development without Docker
```

`scripts/doctor` checks the pinned Rust toolchain and components and prepares
checksum-verified protoc in `target/tools`, uv-managed Python and the locked
`connectors/.venv`. Default mode also prepares the S3 fixture image. Local
mode warns about missing CI-only mbx and cargo-deny. It is idempotent.

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

`cd connectors && uv run --locked --all-extras mypy` checks the SDK, bundled
providers and example provider. Bodies are checked with partial signature
annotations; PyArrow's untyped API remains covered by Arrow contract tests.

## Layout

| Path | Content |
| --- | --- |
| `crates/fyrr-core` | Contracts, configuration, MCP tool schemas. No S3 or Lance. |
| `crates/fyrr-reader` | Pinned Lance reads, authorization, hybrid search, read-through mirror, MCP over stdio and HTTP. |
| `crates/fyrr-writer` | Full build only: provider discovery, Python workers, batch validation, committer, maintenance, directory and SCIM. |
| `crates/fyrr` | The `fyrr` binary; feature `writer` (default) is the full build. |
| `connectors/` | Python package `fyrr-connectors`: provider API, test kit and the bundled providers. |
| `examples/` | The quickstart (configuration, documents and `run`), an independently built provider wheel and a derived-image Dockerfile. |
| `schemas/`, `fixtures/` | Generated JSON Schemas, golden contract documents and MCP goldens. |
| `docker/` | `Dockerfile` (targets `fyrr-reader` and `fyrr`), entry point, `compose.yaml`. |
| `deny.toml` | License and advisory policy. |

## Verifying

```sh
scripts/ci --quick      # before a PR: core gates and local security scenarios
scripts/ci              # full suite: storage, connectors or maintenance changes
scripts/ci --images     # full suite plus image checks (compiles release binaries)
scripts/ci --jobs=2     # bound concurrent Rust cases (default: CPU/memory based)
scripts/ci --step=configuration "--step=schema drift" # exact names; others n/e
scripts/ci --step="reference docs" # standalone reference-doc verification
```

`scripts/ci` runs doctor, rustfmt, clippy for the workspace and the
reader-only build, the test-case ownership guard, unit, contract,
configuration and schema tests in the full build, CLI and privacy tests in
both builds, the debug
reader-only link check, `cargo deny`, the Python SDK and example
provider, the quickstart on local storage, local-storage scenarios,
and a small external-services set against owned RustFS and Keycloak fixtures.
It keeps going after a failure and writes
`target/ci-logs/<UTC stamp>/` with one log per step and `summary.txt`.

Build helpers use pinned mr-boxington (`mbx`) with rustup's Cargo first on
`PATH`. Each test feature set is built once; cases execute preserved full
and reader-only artifacts in a bounded pool. Set `--jobs=N` or
`FYRR_CI_JOBS=N` to bound concurrency. Each case owns its fixtures; failed
fixtures and all logs remain for diagnosis (`FYRR_CI_KEEP_FIXTURES=1` also
keeps successful fixtures). Never share a writable `CARGO_TARGET_DIR` or
run two top-level CI processes in one checkout. Compiler actions can be
shared across worktrees; their Cargo outputs stay private.

`scripts/ci-cases.json` owns the cases and their `quick`/`full` tiers; the
ownership gate checks all cases, including those excluded from the selected tier.

Run `scripts/ci --quick` before a PR. It runs all core gates, CLI and privacy
cases, the Python SDK and example, local quickstart, and the local scenarios
for permission filtering, revocation, fail-closed authorization and CAS/leases.
Its `doctor --local` preflight does not require Docker or fetch service images.
The full `scripts/ci` is required when changing storage, connectors or
maintenance; it adds the remaining local scenarios, providers, real S3,
Keycloak and long maintenance/history campaigns. Cases excluded by the tier
are `n/e` in `case-plan.json` and row results; wholly excluded rows are `n/e`
in `summary.txt`. A mixed row counts only its selected cases as passed.

PRs and pushes to main run `--quick`; `v*` tags run the full tier and then
build the release artifacts and images in a separate job (see
[Releasing](#releasing)).
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
| `scripts/check-reader-only` | Proves the reader-only build links no writer, Python or PyO3. |
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
  `scripts/ci-cases.json`. When you add, rename or remove a test, update its
  row in the same change; `scripts/ci-cases check` fails otherwise.
- **Contracts.** `schemas/`, the SDK's schema copies and `fixtures/mcp/` are
  generated from the Rust types. Regenerate them with
  `FYRR_UPDATE_GOLDEN=1 cargo test --locked -p fyrr-core --test generated`
  and review the diff. An additive change (a new optional field) bumps the
  minor version; anything else bumps the major, and published data is then
  republished, not migrated. The SDK's `versions.py` must match the
  contracts it ships. The configuration schema has its own
  `x-fyrr-schema-version`, currently `5.0` (S3 and local storage).
- **Dependencies.** A new dependency needs a license and advisory verdict in
  `deny.toml`.
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
- One change per commit: a behavior fix and an unrelated refactor are
  separate commits.
- No chronology, machine names, local paths, credentials or ticket
  identifiers in code, docs or messages. Documentation describes current
  behavior.
- Do not commit `fyrr.toml`, `.env` files or build outputs.

## Releasing

`scripts/release` uploads nothing. Pushing a `v*` tag that matches the
workspace version in `Cargo.toml` starts `.github/workflows/release.yml`: it
runs CI (`scripts/ci`, full tier) and, only if CI passes, runs
`scripts/release` once per Linux platform (linux-x86_64 and linux-aarch64)
and `scripts/release-macos` for macos-aarch64, which has archives and no
images. Each
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
(`blacksmith-16vcpu-ubuntu-2404`, `blacksmith-16vcpu-ubuntu-2404-arm` and
`blacksmith-6vcpu-macos-15`), which requires the Blacksmith GitHub App on
the repository. To use other runners, set the repository variables
`RELEASE_RUNNER_X86_64`, `RELEASE_RUNNER_AARCH64` and `RELEASE_RUNNER_MACOS`
to labels with at least 24 GB of memory; `CI_RUNNER_MACOS` selects the
runner of the macOS CI job. Running
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
