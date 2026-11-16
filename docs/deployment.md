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

## Topology

A deployment is one S3 namespace, any number of writer processes and any
number of reader processes. S3 is the only coordination channel: writers on
different hosts take expiring leases per source and sync kind, one committer
at a time holds the namespace lease, and readers only read.

- **Writer hosts** run the full build (or the `fyrr` image) with the provider
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
   `[identity.oidc]` and `[reader.http]` as in
   [HTTP login and users](http-users.md). The embedding
   model and dimensions bind the namespace; see
   [upgrades](operations.md#upgrades).
3. **Writer.** On a host with the full build or the `fyrr` image, run
   `fyrr writer namespace create` once, then `fyrr writer serve` as a
   service, and schedule `fyrr writer maintain` (for example every six
   hours), which `writer serve` does not run ([scheduling](#scheduling)).
4. **Reader.** Run the `fyrr-reader` image (or the reader-only build) with
   the same file. Over HTTP it listens on plain HTTP: put a TLS-terminating
   proxy in front of it ([start the reader](http-users.md#3-start-the-writer-and-the-reader)). Check readiness
   with `fyrr reader check` or `/readyz`, which answers health probes under
   any `Host` with the status code alone.

## Downloadable binaries

Bare release binaries are dynamically linked Linux x86-64 executables.
Their `binary_runtime` sections in `manifest.txt` list the maximum required
GLIBC symbol version, ELF interpreter and shared libraries. Local assembly
inherits the build host's ABI; official releases use the Ubuntu 24.04
release job. The manifest records the actual build provenance and runtime
requirements. A GLIBC version alone does not guarantee compatibility:
the interpreter and all required libraries must also be available.

The full binary additionally needs the matching CPython shared library
listed in `needed` (3.13 in the standard build) and the matching connector
SDK wheel with its provider extras. Install these in a
Python environment, expose its library directory through the system loader
or `LD_LIBRARY_PATH`, and set `VIRTUAL_ENV` when using that environment's
providers. Release binaries embed no RPATH or RUNPATH. The `fyrr` image
includes this runtime and the bundled providers.

The release job executes the exported binaries in the pinned runtime
images before uploading artifacts; it also checks SDK provider
discovery. Reader-only binaries require no Python.

## Images

Tagged releases will publish `ghcr.io/fyrr-ai/fyrr:<version>` and
`ghcr.io/fyrr-ai/fyrr-reader:<version>`.

`docker/Dockerfile` builds both images from the repository root; every base
image is pinned by digest. Both carry the `LICENSE` and `NOTICE` files under
`/usr/share/doc/fyrr/`.

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
Lance in release mode and takes a while.

### Providers in the image

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

Sync, directory pulls and commits can also run from cron, except for a SCIM
directory, whose freshness only `writer serve` with the administration
listener attests:

```
*/5 * * * *  fyrr writer sync --due
*/5 * * * *  fyrr writer sync --directory --due     # with a snapshot or keycloak directory
*/2 * * * *  fyrr writer commit
0 */6 * * *  fyrr writer maintain
```

Several `sync` processes may run at once, but not on the same source and
kind: the second one reports `sync_busy` for it and exits 1. `commit`,
`maintain` and `sync --directory` share the namespace lease, so whichever
starts second exits 75 at once (see the [exit statuses](cli.md)). A killed
worker repeats at most one batch, because a cursor advances only after its
envelope is durable.
