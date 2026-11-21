# Deployment

> **In short**
>
> - A deployment is one namespace on S3, any number of writer processes and
>   any number of reader processes; S3 is the only coordination channel. A
>   single host may keep the namespace in a local directory instead.
> - This page covers the topology, a minimal production setup, the two
>   container images and how to schedule the writer.
> - Read it when you move from the quickstart to a real bucket and a real
>   embeddings model.

Monitoring, maintenance, restore and upgrades of a running deployment are
in [operations](operations.md).

For an experimental serverless reader, see [Run the reader on AWS Lambda](lambda.md).

## Topology

Writers on different hosts take expiring leases per source and sync kind;
one [committer](architecture.md#terms) at a time holds the namespace lease,
and readers only read.

- **Writer hosts** run the full build (or the `fyrr` image) with the [provider](architecture.md#terms)
  packages and their credentials.
- **Reader hosts** run the reader-only build (or the `fyrr-reader` image):
  one stdio process per user, or one HTTP service for many users. They need
  the embeddings endpoint for live query embeddings and a private mirror
  directory sized by `reader.mirror_max_bytes`.

A single host can use a local directory instead of S3
(`uri = "file:///var/lib/fyrr"`, see [local storage](configuration.md#local-storage)).
The writer and readers then coordinate through that directory, so they all
run on that host as the same user; it cannot be shared with other hosts,
also not over a network file system. Use S3 as soon as a second host needs
the namespace.

When a writer runs as PID 1 in the full image, it also reaps orphaned
processes during worker cleanup. Run unrelated processes in separate
containers.

## Minimal production setup

Moving from the [quickstart](quickstart.md) to a deployment changes four
things: a real bucket, a real embeddings model, a reader without the writer,
and a writer that runs on its own.

1. **Bucket.** Create an S3 bucket (or a prefix in one). Writers need read,
   write and delete access to the prefix; readers never write to S3, so give
   them read-only credentials. Without `access_key_id`, credentials come
   from AWS environment variables, web identity, container credentials or
   instance metadata; AWS shared profile files are not loaded.
2. **Configuration.** One file, shared by writers and readers:

   ```toml
   [namespace]
   name = "acme"

   [storage]
   uri = "s3://acme-fyrr/prod"
   region = "eu-central-1"

   [embedding]
   endpoint = "https://embeddings.example.com/v1"   # any OpenAI-compatible /embeddings API
   model = "BAAI/bge-m3"
   dimensions = 1024
   api_key = { env = "FYRR_EMBEDDING_API_KEY" }
   reader_live = true                               # embed queries; off serves lexical only

   # Millionths of a US dollar. The cap applies per reader process, per
   # `writer sync` run (split evenly across its sources) and per source run
   # under `writer serve`.
   [embedding.spend]
   limit_microusd = 50000000
   max_call_microusd = 2000                         # your bound on one embeddings request

   [identity]
   hashing_key = { env = "FYRR_IDENTITY_KEY" }      # the same value on every host
   encryption_key = { env = "FYRR_ENCRYPTION_KEY" } # 64 hexadecimal characters

   [reader]
   mirror_path = "/var/lib/fyrr/mirror"             # default: $XDG_CACHE_HOME/fyrr/<namespace>

   [[sources]]
   id = "handbook"
   type = "local-files"
   permissions = "fixed"
   readers = ["grp:engineering"]
   [sources.scope]
   paths = ["/srv/handbook"]
   ```

   For one user over stdio, add `stdio_user` and `stdio_readers` to
   `[identity]`; for many users over HTTP, add a directory,
   `[identity.oidc]` (or API keys, which need no directory) and
   `[reader.http]` as in
   [HTTP login and users](http-users.md). The embedding
   model and dimensions bind the namespace; see
   [upgrades](operations.md#upgrades).
3. **Writer.** On a host with the full build or the `fyrr` image, run
   `fyrr writer namespace create` once, then `fyrr writer serve` as a
   service ([scheduling](#scheduling)).
4. **Reader.** Run the `fyrr-reader` image (or the reader-only build) with
   the same file. Over HTTP it listens on plain HTTP: put a TLS-terminating
   proxy in front of it ([start the reader](http-users.md#3-start-the-writer-and-the-reader)). Check readiness
   with `fyrr reader check` or `/readyz`, which answers health probes under
   any `Host` with the status code alone.

## Downloadable binaries

Download `fyrr` (and `fyrr-reader`) for your platform and run it: the
binaries are dynamically linked Linux executables for x86-64
(`linux-x86_64`) and 64-bit ARM (`linux-aarch64`), and need nothing but the
system C library. Neither contains a connector or a Python interpreter, so
`fyrr --version`, `fyrr reader serve`, `fyrr config check` for a
configuration without sources and every other command that does not run a
connector work on a machine with no Python at all. Linux is the only
supported platform. Each platform's `manifest-<platform>.txt` has
`binary_runtime` sections that list the maximum required GLIBC symbol
version, ELF interpreter and shared libraries; official releases use
Ubuntu 24.04 release jobs and local assembly inherits the build host's ABI.
A GLIBC version alone does not guarantee compatibility: the interpreter and
all required libraries must also be available.

To sync from a source, install the connector SDK into a Python virtual
environment and let `fyrr` find it. The package index has
`fyrr-connectors` 0.1.0, and fyrr 0.1.1 shipped a 0.1.1 wheel; both were
built for the embedded interpreter of earlier fyrr versions, have no provider
host and do not work with this fyrr. This fyrr needs the SDK wheel of its
own release (0.1.2 or newer), which ships next to the binaries
(`fyrr_connectors-<version>-py3-none-any.whl`). Install that wheel, not the
package from the index (the host refuses an SDK that speaks another
provider-host protocol), with the extras of the provider types you use:

```sh
python3 -m venv /opt/fyrr-connectors
/opt/fyrr-connectors/bin/pip install './fyrr_connectors-<version>-py3-none-any.whl[jira]'
export VIRTUAL_ENV=/opt/fyrr-connectors
fyrr writer providers
```

The commands that run connectors (`writer sync`, `writer providers`,
`writer serve` jobs, and the provider checks of `config check` and
`config schema`) start a Python provider host from the first of these that
exists: `$VIRTUAL_ENV/bin/python`; a `python3` next to the `fyrr`
executable; `python3` on `PATH`. A `VIRTUAL_ENV` that is set but has no executable
`bin/python` stops the search with an error; it does not fall back to the
other two. If none is found, or it cannot import
`fyrr_connectors` (or finds an SDK that does not match this `fyrr`), they
stop with one message that says to install the wheel of this release into a
virtual environment and set `VIRTUAL_ENV`. The message comes from a worker
that starts the host under the same limits as every other worker, never
from a Python that the `fyrr` process itself ran. An interpreter is never
run from the current directory. The connector SDK needs
Python 3.11 or newer (see [the connectors guide](connectors/README.md#python-versions));
nothing else about the interpreter has to match the `fyrr` binary.

`scripts/release` fails if the full binary's `needed` libraries include
libpython, and runs it with an empty environment. The `fyrr` image includes
the interpreter, the SDK and the bundled providers.

## Images

Tagged releases publish `ghcr.io/fyrr-ai/fyrr:<version>` and
`ghcr.io/fyrr-ai/fyrr-reader:<version>` as multi-arch images for
`linux/amd64` and `linux/arm64`; `<version>-amd64` and `<version>-arm64`
name one architecture. The release also carries each image as an archive
per platform.

`docker/Dockerfile` builds both images from the repository root; every base
image is pinned by digest. Both carry the `LICENSE` and `NOTICE` files under
`/usr/share/doc/fyrr/`. Release images package the binaries of the release
job; a build from a checkout compiles them in the pinned builder stage.

| Target | Base | User | Content |
| --- | --- | --- | --- |
| `fyrr-reader` | distroless `cc-debian13:nonroot` | 65532 | the reader-only binary; no Python, no shell |
| `fyrr` | `python:3.13.17-slim-trixie` | 10001 | the full binary, the connector SDK with the bundled providers' extras, and `fyrr-entrypoint` |

```sh
docker build --file docker/Dockerfile --target fyrr-reader --tag fyrr-reader:local .
docker build --file docker/Dockerfile --target fyrr --tag fyrr:local .
docker run --rm -v "$PWD/fyrr.toml:/etc/fyrr/fyrr.toml:ro" fyrr-reader:local --config /etc/fyrr/fyrr.toml config check --no-env
```

`--no-env` skips the check that every referenced environment variable is
set; pass the variables with `-e` and drop it to check them too.

`scripts/images` builds and tests both images, including a real publication
on the local S3 fixture and `docker/compose.yaml`. The first build compiles
[Lance](architecture.md#terms) in release mode and takes a while.

To package binaries you already built instead of compiling, name their
directory; it must hold `fyrr` and `fyrr-reader`, built for the runtime
images' glibc or older:

```sh
docker build --file docker/Dockerfile --target fyrr --tag fyrr:local \
  --build-arg BINARIES=prebuilt --build-context release=path/to/binaries .
```

### Providers in the image

From a checkout, discover providers with
`VIRTUAL_ENV=$PWD/connectors/.venv target/debug/fyrr writer providers`.
To try a wheel locally, build it as below, install it with
`uv pip install --no-deps --python connectors/.venv/bin/python --target /tmp/site target/example-wheels/*.whl`,
then add `PYTHONPATH=/tmp/site` to the discovery command.

External providers are installed without rebuilding fyrr, either in a
derived image or from a mounted wheel directory:

```sh
uv build --wheel --out-dir target/example-wheels examples/markdown-provider
docker build -f examples/derived-image/Dockerfile --build-arg FYRR_IMAGE=fyrr:local -t my-fyrr target/example-wheels
docker run --rm -e FYRR_PROVIDER_WHEELS=/wheels -v "$PWD/target/example-wheels:/wheels:ro" fyrr:local writer providers
```

`writer providers` exits 1 when an installed provider is refused (for example
one requiring another SDK series); a source on a refused provider fails
`config check` with status 78.

## Scheduling

The simplest writer is one long-running process:

```sh
fyrr writer serve --jobs 4 --timeout 10m
```

It syncs each source's kinds when their configured intervals are due, pulls
the directory on its interval, and commits every
`writer.commit.coalescing_timer` (60 s). `SIGINT` or `SIGTERM` stops the
workers, finishes the current commit group and releases the lease. It does
not run maintenance: schedule `fyrr writer maintain` next to it.

Sync, directory pulls and commits can also run from cron (a SCIM directory
needs `writer serve` with the administration listener to receive pushes):

```
*/5 * * * *  fyrr writer sync --due
*/15 * * * *  fyrr writer sync --directory       # with a snapshot or keycloak directory
*/2 * * * *  fyrr writer commit
0 */6 * * *  fyrr writer maintain
```

Several `sync` processes may run at once, but not on the same source and
kind: the second one reports `sync_busy` for it and exits 1. `commit`,
`maintain` and `sync --directory` share the namespace lease, so whichever
starts second exits 75 at once (see the [exit statuses](cli.md)). A killed
worker resumes from its durable cursor after its source/kind lease expires
(up to `writer.commit.lease_ttl` after the last renewal). A cursor advances only
after its envelope is durable; an interrupted current batch can be replayed.
Completed envelopes remain pending and can be committed independently. Replayed
text can reuse vectors already persisted in `embedding.local_cache`; in-flight
embedding requests may still have been billed and are not refunded. Provider
cursor semantics also apply: local-files scans the tree again and skips known
raw-byte revisions. `--full` deliberately resets the provider cursor, but keeps
durable envelopes and reusable vectors.

The per-source worker limit stays `--timeout 10m` for all syncs, including
`--full`. For an initial embedding-heavy corpus, allow enough time explicitly:

```sh
fyrr writer sync --full --timeout 1h
fyrr writer commit
```

A timeout exits 1 with `worker_failure: timeout after <N>ms; increase --timeout`.
Only the configured duration is printed; provider exceptions remain private.
Restart with a larger timeout after the lease expires; do not delete pending
envelopes or the vector cache.
