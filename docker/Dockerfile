# The two images of the one fyrr binary:
#   --target fyrr-reader  reader-only build: no writer, Python or PyO3
#   --target fyrr         full build with the embedded Python, the connector
#                         SDK and the bundled providers
# Build from the repository root; scripts/images builds, tests and measures
# both. Base images are pinned by digest; both runtimes are Debian 13, like
# the builder, so the binary's glibc and libpython match.
ARG RUST_IMAGE=rust:1.97.1-trixie@sha256:b1b3c9c0d921d7fa0a6d1f9ec7e4eab87f8c8ec97644c3d791450f131dec813f
ARG PYTHON_BUILD_IMAGE=python:3.13.17-trixie@sha256:d5a186865324088bd10d1671801b42297a8fe3f8059143fb22b2e2b776ffa6dd
ARG PYTHON_IMAGE=python:3.13.17-slim-trixie@sha256:d98cc29d3266e535dd9da1099688d9cb3ca4753813571e071763d17725460962
ARG READER_IMAGE=gcr.io/distroless/cc-debian13:nonroot@sha256:cb87acc574abe719b6e5c547384defa2f7b1f24af9605d3f618cf852495d8be2
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.12.21@sha256:a7aed3216253ee804de3e2d8afa5073baa1a177335345d43845cd4165e43b711

FROM ${RUST_IMAGE} AS rust
FROM ${UV_IMAGE} AS uv

# Builds both binaries in one cargo target (cache mounts keep the registry,
# git checkouts and target between builds). The Python of the full image's
# base links PyO3, so libpython matches at run time.
FROM ${PYTHON_BUILD_IMAGE} AS builder
ARG CARGO_BUILD_JOBS=4
COPY --from=rust /usr/local/cargo /usr/local/cargo
COPY --from=rust /usr/local/rustup /usr/local/rustup
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUSTUP_TOOLCHAIN=1.97.1 \
    PYO3_PYTHON=/usr/local/bin/python3.13 \
    CARGO_TARGET_DIR=/cache/target
WORKDIR /src
# The pinned, checksum-verified protoc that Lance's build scripts need.
COPY scripts/lib.sh scripts/pins.env scripts/
RUN bash -c 'source scripts/lib.sh && ensure_protoc'
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo .cargo
COPY crates crates
# A workspace member; the images never build it.
COPY eval eval
# Worktrees can supply source mtimes older than the shared Cargo cache.
# Refresh workspace inputs under the cache lock; versioned dependencies stay cached.
RUN --mount=type=cache,id=fyrr-cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=fyrr-cargo-git,sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=fyrr-cargo-target,sharing=locked,target=/cache/target \
    bash -c 'source scripts/lib.sh; mkdir -p /out; \
    find crates eval -type f -exec touch {} +; \
    build_release -p fyrr; \
    cp /cache/target/release/fyrr /out/fyrr; \
    build_release -p fyrr --no-default-features; \
    cp /cache/target/release/fyrr /out/fyrr-reader'

# The connector SDK wheel and its pinned runtime requirements (uv.lock, with
# hashes): the SDK's own and those of the bundled providers that are built
# (google-drive, notion, github, slack and jira extras). The testkit extra and dev tools stay out.
FROM ${PYTHON_BUILD_IMAGE} AS sdk
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_PREFERENCE=only-system UV_PYTHON_DOWNLOADS=never
COPY connectors /src/connectors
RUN uv build --wheel --out-dir /dist /src/connectors \
    && uv export --project /src/connectors --frozen --no-dev --no-emit-project \
        --extra google-drive --extra notion --extra github --extra slack --extra jira \
        --format requirements-txt --output-file /dist/requirements.txt

FROM ${READER_IMAGE} AS fyrr-reader
COPY --from=builder /out/fyrr-reader /usr/local/bin/fyrr
COPY LICENSE NOTICE /usr/share/doc/fyrr/
# distroless :nonroot runs as uid 65532.
USER 65532:65532
LABEL org.opencontainers.image.source="https://github.com/fyrr-ai/fyrr" \
      org.opencontainers.image.licenses="Apache-2.0"
ENTRYPOINT ["/usr/local/bin/fyrr"]
CMD ["--help"]

FROM ${PYTHON_IMAGE} AS fyrr
RUN --mount=type=bind,from=sdk,source=/dist,target=/dist \
    pip install --no-cache-dir --disable-pip-version-check --require-hashes --no-deps \
        -r /dist/requirements.txt \
    && pip install --no-cache-dir --disable-pip-version-check --no-deps /dist/fyrr_connectors-*.whl \
    && useradd --uid 10001 --create-home --home-dir /home/fyrr --shell /usr/sbin/nologin fyrr
COPY --from=builder /out/fyrr /usr/local/bin/fyrr
COPY LICENSE NOTICE /usr/share/doc/fyrr/
COPY docker/fyrr-entrypoint /usr/local/bin/fyrr-entrypoint
USER 10001:10001
WORKDIR /home/fyrr
LABEL org.opencontainers.image.source="https://github.com/fyrr-ai/fyrr" \
      org.opencontainers.image.licenses="Apache-2.0"
ENTRYPOINT ["/usr/local/bin/fyrr-entrypoint"]
CMD ["--help"]
