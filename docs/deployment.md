# Deployment

> **In short**
>
> - A deployment is one namespace on S3, any number of writer processes and
>   any number of reader processes; S3 is the only coordination channel. A
>   single host may keep the namespace in a local directory instead.
> - This page covers the topology, a minimal production setup, the two
>   container images, the Helm chart and how to schedule the writer.
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

## Kubernetes

`deploy/helm/fyrr` is a Helm chart for one namespace: a `Deployment` of
readers behind a `ClusterIP` `Service`, and exactly one writer. Install it
from a checkout (helm 3):

```sh
helm install fyrr deploy/helm/fyrr --values my-values.yaml
```

- **Readers.** `reader.replicas` pods of the `fyrr-reader` image run
  `fyrr reader serve`. Readiness is `GET /readyz` on `reader.port`, which
  must be the port of `reader.http.listen` in the configuration (bind
  `0.0.0.0`). The `Service` forwards that port. The chart has no ingress
  and no `LoadBalancer`: the reader speaks plain HTTP and refuses every
  request with an `Origin` header, so it must not be exposed raw to
  browsers. Put a TLS-terminating gateway in front that keeps the public
  `Host` ([HTTP boundary](http-users.md#http-boundary)).
- **Writer.** One pod of the `fyrr` image runs `fyrr writer serve`. The
  `Deployment` has one replica and the `Recreate` strategy, so an upgrade
  never runs two writers. The readers become ready only after the first
  content publication, and with API keys they refuse every request until
  the keys are published. After the first install, run once, in this order
  (release name `fyrr`; the writer's secret must hold the keys' variables):

  ```sh
  w="kubectl exec deploy/fyrr-writer -- fyrr --config /etc/fyrr/fyrr.toml writer"
  $w namespace create
  $w sync
  $w commit
  $w keys        # required with [[identity.api_keys]]
  ```

  The running `writer serve` also syncs and commits on its own schedule, so
  `sync` or `commit` may report that another process holds the lease; repeat
  the command (see [Scheduling](#scheduling)). A one-off `writer sync` for a
  large first load needs `--timeout`.
  The writer's administration listener (`writer.serve.admin`, for SCIM) has no
  `Service` in the chart.
- **Configuration.** `config` is the `fyrr.toml` of both, mounted from a
  `ConfigMap` at `/etc/fyrr/fyrr.toml`; a change restarts the pods. Leave
  `reader.mirror_path` unset.
- **Secrets.** Never put a secret in `config` or in the values: refer to
  it as `{ env = "NAME" }` and provide `NAME` from an existing `Secret`
  with `reader.existingSecret` and `writer.existingSecret` (every key
  becomes a variable), or with `reader.env` and `writer.env` (Kubernetes
  `EnvVar` entries, for example with `valueFrom.secretKeyRef`). The two
  pods get separate secrets: give the readers read-only bucket credentials
  (or a `serviceAccountName` that grants them), and keep provider
  credentials and write access to the bucket with the writer.
- **Disk.** Both pods run as the image's non-root user with a read-only
  root file system, no capabilities and no service-account token. The
  readers' mirror is an `emptyDir` at `/cache` (`XDG_CACHE_HOME`), sized by
  `reader.mirrorSizeLimit`, which must exceed `reader.mirror_max_bytes`; it
  is a read-through cache, rebuilt from S3 after a restart. The writer gets
  `emptyDir`s at `/tmp` and `/cache` for its scratch space and vector cache;
  a new writer pod embeds again what the cache held.
  `FYRR_PROVIDER_WHEELS` is not available on the read-only file system:
  [derive an image](#providers-in-the-image) instead.
- **Metrics.** `metrics.port` opens a pod port named `metrics` on every
  pod. Also set `reader.http.metrics_listen` and `writer.serve.metrics_listen`
  to `0.0.0.0:<port>` in `config`, and scrape the pods; the endpoint has no
  authentication, so the `Service` does not carry it
  ([Metrics](operations.md#metrics)).

The chart's `version` and `appVersion` are the workspace version, and the
default image tag is the `appVersion`: the release that ships this chart.
Before that release is published, build the images with `scripts/images`
and set `reader.image.tag` and `writer.image.tag`.

`scripts/images` lints and renders the chart (`helm lint --strict` and
`helm template`, with helm pinned in `scripts/pins.env`) before it builds
the images.

## Many namespaces behind a load balancer

A reader opens a namespace on the first request that names it in the path
(`/v1/namespaces/{namespace}/...`) and mirrors it from then on. Behind a plain
load balancer every reader sees requests for every namespace, so each one
mirrors all of them: each cold start and each mirrored byte is paid once per
reader. To keep a namespace on few readers, route on the namespace.

- **Hash on the namespace.** Hash the path segment after `/v1/namespaces/`
  (consistent hashing, so adding or removing a reader moves few namespaces).
  Requests for one namespace then reach the same reader, or with a hash over
  the namespace and a small replica index, the same N readers.
- **Split by route, not only by method.** Send `GET` and `HEAD` on
  `/v1/namespaces/...` and `POST` on `/v1/namespaces/{namespace}/query`,
  `/fetch`, `/aggregate` and `/recall` to the readers. Send every other
  request on `/v1/namespaces/...` (`POST .../documents`, `PATCH` and `DELETE`
  on `/v1/namespaces/{namespace}`) to the writer listener. A rule that sends
  every non-`GET` request to the writer would send the search routes there;
  they are `POST` and belong to the readers.
  `GET` and `HEAD /v1/namespaces`, the namespace list, has no namespace
  segment: send it to any reader without hashing. The writer listener answers
  it `404`.
- **`/mcp` has no namespace in its path.** A reader serves `/mcp` from the
  namespace in its own configuration, so never hash it into a shared pool of
  readers. Route `/mcp` by host, or give each configured namespace its own
  pool of readers. With OIDC, send the two OAuth discovery paths,
  `/.well-known/oauth-protected-resource` and
  `/.well-known/oauth-protected-resource/mcp`, to the same pool as `/mcp`:
  another reader answers them `404`, and a client then cannot sign in.
- **No coordination.** fyrr adds no router process: the hashing is the load
  balancer's. Readers never coordinate and never learn which namespaces
  another reader serves; each decides from the requests it receives.
- **A failed reader.** When the balancer drops a reader, it hashes that
  reader's namespaces to the others. With API keys, the next reader opens such
  a namespace from storage and answers the first query cold, then serves it
  from its own mirror (see the OIDC limit below). Nothing is lost, because a
  reader holds only a read-through copy of storage.

With API keys, and the same configuration on every reader, any reader serves
any namespace, so a namespace sent to a reader that did not open it yet is not
an error; the hashing only decides how many readers pay for each one. This
holds only with API keys. A request authenticated with OIDC is served only for
the namespace the reader is configured for (`namespace.name`); for any other
namespace the reader answers `503`, so it cannot take over another reader's
namespaces. With OIDC, route each namespace to readers configured for it.
An OIDC caller also holds no `list` or `admin` role, so the namespace list,
the namespace report and `recall` answer `403` at the role check.

## Scheduling

The simplest writer is one long-running process:

```sh
fyrr writer serve --jobs 4 --timeout 10m
```

It syncs each source's kinds when their configured intervals are due, pulls
the directory on its interval, and commits every
`writer.commit.coalescing_timer` (60 s). `SIGINT` or `SIGTERM` stops the
workers, finishes the current commit group or maintenance step and releases the
lease. It also runs `fyrr writer maintain`, outside the commit loop, when the served version needs it
(too many index segments at once; fragments with deletions, or no maintenance
for longer than the windows after which retired versions and applied envelopes
are deleted, at most once per `writer.version_grace`). A write that waits for the lease
ends that run after its current step; the purge is never deferred by more than one
`writer.version_grace` interval. Set `writer.serve.maintenance = false` to
schedule `fyrr writer maintain` yourself, as below.

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
