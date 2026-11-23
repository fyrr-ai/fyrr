# Identity and access configuration

> **In short**
>
> - The `[identity]` table: the stdio subject, the hashing and encryption
>   keys, and how long permission and directory reads stay valid.
> - `[identity.directory]` selects where [canonical users](architecture.md#terms) and groups come from;
>   `[identity.oidc]` holds the OIDC settings for HTTP MCP and
>   `[[identity.api_keys]]` declares keys with explicit grants.
> - Read it when you set up who may read: one user over stdio, or many users
>   over HTTP.

The rest of the configuration file is described in
[configuration](configuration.md), [tuning](tuning.md) and
[reader and writer settings](reader-writer.md); what these keys protect is
in the [security model](security.md).

## `[identity]`

| Key | Default | Meaning |
| --- | --- | --- |
| `stdio_user` | absent | The subject of the stdio MCP transport (one process, one identity). |
| `stdio_readers` | `[]` | Reader tokens (`usr:<id>`, `grp:<id>`) of the stdio subject. |
| `hashing_key` | required | Namespace HMAC key, `{ env = "VAR" }`; the same value on every writer and reader. Only hashed reader tokens and [principal](architecture.md#terms) keys enter the published artifacts and rows. |
| `encryption_key` | absent | 32 random bytes as 64 hexadecimal characters, `{ env = "VAR" }`; a separate secret from `hashing_key`. Encrypts persisted identity inputs (directory snapshots, membership, SCIM state, stored credentials). `config check` requires it with a `permissions = "sync"` source, a `snapshot` or `keycloak` directory, or writer administration. |
| `max_tokens_per_user` | `5000` | At least 1. A subject with more reader tokens gets `authorization_unavailable`; other subjects are unaffected. |
| `oidc` | absent | OIDC settings for HTTP MCP, see below. |
| `api_keys` | `[]` | `[[identity.api_keys]]` tables: bearer keys for HTTP MCP, see below. |
| `directory` | see below | Canonical directory. |

### `[[identity.api_keys]]`

An API key lets a client without an identity provider use `[reader.http]`.
The operator declares what each key may read, as `stdio_user` and
`stdio_readers` do for stdio. [HTTP login and users](http-users.md#api-keys)
walks through it.

| Key | Default | Meaning |
| --- | --- | --- |
| `name` | required | Unique ID like a source ID (`[a-z0-9][a-z0-9_-]{0,62}`). It names the key in this file; the artifacts hold only its hash. |
| `key` | required | `{ env = "VAR" }`: the secret, `fyrr_key_` followed by at least 43 characters from `[A-Za-z0-9_-]`. Generate it with `echo "fyrr_key_$(openssl rand -hex 32)"`. Every key needs its own value. |
| `readers` | `[]` | Required with the `read` role: at least one reader token (`usr:<id>`, `grp:<id>`) the key acts with, as `stdio_readers`, including source-scoped tokens for `permissions = "sync"` sources. A key without the `read` role needs none. |
| `roles` | `["read"]` | What the key may do over HTTP: `read` (search and fetch, over `/mcp` and the [JSON API](http-api.md)), `write`, `list` and `admin`. At least one. A route whose role the key lacks answers `403 forbidden`. `admin` allows `DELETE /v1/namespaces/{namespace}` on the writer's administration listener, which deletes the namespace and its data; the name must be repeated in the JSON body (`{"namespace":"NAME"}`). It also allows `PATCH /v1/namespaces/{namespace}` there with `{"read_only":true}`, which [freezes the namespace](http-api.md#freezing-a-namespace) (`false` thaws it). `list` or `admin` allows `GET /v1/namespaces/{namespace}` on the reader, the [namespace report](http-api.md#namespace-report), and `GET /v1/namespaces`, the [namespace list](http-api.md#listing-namespaces), which shows the names under the key's `namespace_prefix` only. `admin` with `read` allows `POST /v1/namespaces/{namespace}/recall`, the [recall report](http-api.md#recall) of the vector index. Only `read`, `list` and `admin` have reader routes today; the other roles are granted so that later routes need no new key. |
| `namespace_prefix` | absent | The key acts only in namespaces whose name starts with this (at most 63 characters from `[a-z0-9-]`). In any other namespace it answers `403 forbidden`. |

Only the **writer** reads the secret, so the reader needs no key in its
environment. Every host that publishes identity needs every key's variable:
a host without one publishes identity without that key's binding. `config
check` reports a variable that is unset, a secret of the wrong shape and two
keys with the same secret; `config check --no-env` skips that. The writer
skips a key whose secret is missing or malformed and publishes every other
key; two keys with the same secret stop its commit.

`fyrr writer keys` publishes the keys of the configuration, each as a keyed
hash with its roles and prefix, in one object under the storage root
(`api-keys.json`, above `namespaces/`) by compare-and-swap; it writes nothing
when the object is current. It is a required step: a reader that declares
keys refuses every API key (`503 authorization_unavailable`) until it can read
the object, and refuses a key that is not in it (`401`). The object is the
deployment's whole key set, so run the command from a configuration that
declares every key, on a host that has all their variables; a key whose
variable is unset or malformed fails the run. The hash in it names no
namespace, so the namespaces of a deployment share one `hashing_key`.

What a key may read still comes from `readers` in each namespace's identity,
published by `writer commit` (a key is bound only where it holds `read` and
its prefix matches the namespace name), so declare the key in the
configuration of every namespace where it reads. A reader reads the object
when it starts and at every version check interval (`POST /admin/refresh`
too), one more `GET` each, and only then: a request never reads it. It
refuses every key (`503`) while its copy is older than two intervals or
missing.

`reader serve` over HTTP needs `[identity.oidc]`, an API key or both, and
refuses to start without them; `config check` warns about it. The writer does
not need either, so removing the last key still publishes and revokes it.

### `[identity.directory]`

| Key | Default | Meaning |
| --- | --- | --- |
| `provider` | `none` | `none`, `snapshot`, `keycloak` or `scim`. |
| `sync_interval` | `15m` | Pull interval of `writer serve` for the `keycloak` and `snapshot` providers. SCIM pushes need none. |
| `credentials` | absent | `{ env = "VAR" }`: a Keycloak admin bearer token, a Keycloak client secret when `client_id` is set, or the SCIM [provider](architecture.md#terms) credential. |
| `endpoint` | absent | Keycloak base URL (HTTPS, or explicitly allowed loopback). |
| `realm` | absent | Keycloak realm. |
| `client_id` | absent | Optional Keycloak service-account client. |
| `issuer` | absent | Issuer under which the directory binds OIDC subjects; required for SCIM. A Keycloak directory without it uses `<endpoint>/realms/<realm>`. |
| `verified_domains` | `[]` | Corporate e-mail domains accepted for canonical users; required for SCIM and Keycloak. Slack and Jira sources without `settings.corporate_domains` use the same list. |
| `snapshot_file` | absent | A complete directory snapshot file for `provider = "snapshot"`. |

The reader holds no directory clock: a published directory read is served
until a newer publication replaces it. A manual `writer directory --snapshot`
import is accepted under any provider. It must be built after the last
published directory read (its `built_at` not older than that read's): an older
one is refused as stale (`directory_snapshot_stale`). An unchanged pull or
import publishes nothing.

Keycloak directory snapshots are bounded to 100,000 users, 100,000 groups
and 1,000,000 combined user/group membership edges. Each user needs two
membership count checks and a paged membership read, plus directory and
group scans. The whole pull uses `--timeout` (default `10m`).

Keycloak membership reads can miss changes during a pull; see [source revocation timing](security.md#source-polling).

A pull uses one access token and does not refresh it mid-read. Its remaining
lifetime must exceed the read duration; increasing `--timeout` does not
extend that lifetime. With `client_id`, configure the service-account
client's token lifespan in Keycloak for the expected read duration. The
short-lived admin-token example suits small realms. Partial or failed
pulls publish no directory snapshot.

### Other directory providers

- `scim`: pushes users and groups to `[writer.serve.admin]`, binding subjects
  under `identity.directory.issuer` (which must match the OIDC issuer).
  At most 100,000 live users and groups combined; bodies at most 1 MiB.
  PUT, PATCH and DELETE require `If-Match: <meta.version>` (428 if missing,
  412 if stale). Lists support `startIndex`, `count` up to 1,000 and `eq`
  filters on `userName` (users only), `displayName`, `id` or `externalId`.
  Projections, sorting and bulk operations are unsupported. Map `subjects`
  (`issuer`, `subject`) in the custom user extension
  `urn:fyrr:params:scim:schemas:extension:identity:2.0:User` and support
  conditional writes.
- `snapshot`: pulls a complete hashed directory snapshot from `snapshot_file`
  (`schemas/directory-snapshot.schema.json`); subject bindings name issuers.

### `[identity.oidc]`

| Key | Default | Meaning |
| --- | --- | --- |
| `issuer` | required | Exact `iss` of accepted tokens. With a `keycloak` directory it defaults to the directory issuer. With a `keycloak` or `scim` directory it must equal that issuer, which binds every subject; `config check` refuses a mismatch. |
| `audience` | required | The token's `aud`, a string or an array, must contain it. |
| `jwks_url` | required | Operator-pinned key endpoint; token headers never choose a URL. With `keycloak = true` it defaults to `<issuer>/protocol/openid-connect/certs`. |
| `jwks_validity` | `5m` | 1s–300s; maximum trust age after a successful fetch. |
| `jwks_refresh_interval` | `5s` | |
| `scope` | `fyrr:read` | Required scope. |
| `keycloak` | `false` | Accept Keycloak's `JWT`/`Bearer` access-token profile instead of `at+jwt`. On by default when `issuer` comes from a Keycloak directory. |

The derived values come from the configuration file only, never from a
token, and an explicit key always wins. `fyrr config show` prints the
effective values.
