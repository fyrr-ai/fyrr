# Identity and access configuration

> **In short**
>
> - The `[identity]` table: the stdio subject, the hashing and encryption
>   keys, and how long permission and directory reads stay valid.
> - `[identity.directory]` selects where [canonical users](architecture.md#terms) and groups come from;
>   `[identity.oidc]` holds the OIDC settings for HTTP MCP and
>   `[[identity.api_keys]]` declares static keys with explicit grants.
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
| `validity_deadline` | `1h` | How long the last successful permission and member read of a `permissions = "sync"` source, and the last directory read, stay valid. After it the source is hidden (`metadata.hidden_sources`); an expired directory read refuses the whole namespace. The clock follows the publication: a serving head that carries a directory freshness timestamp (a completed pull or SCIM attestation) is held to the deadline by every reader, whatever its own `provider`. A head with none, such as a snapshot imported by hand, is served without a directory clock by a reader with `provider = "none"`. |
| `max_tokens_per_user` | `5000` | At least 1. A subject with more reader tokens gets `authorization_unavailable`; other subjects are unaffected. |
| `oidc` | absent | OIDC settings for HTTP MCP, see below. |
| `api_keys` | `[]` | At most 256 `[[identity.api_keys]]` tables: static bearer keys for HTTP MCP, see below. |
| `directory` | see below | Canonical directory. |

`validity_deadline` must cover the longest permission/member interval or
`identity.directory.sync_interval` (even with `provider = "none"`), plus
`writer.commit.coalescing_timer`, a five-minute margin and, only in
`interval` mode, `reader.version_check_interval`. `config check` prints
the required value.

### `[[identity.api_keys]]`

An API key lets a client without an identity provider use `[reader.http]`:
an unattended agent, or a small team without Keycloak. The operator declares
what each key may read, as `stdio_user` and `stdio_readers` do for stdio.
[HTTP login and users](http-users.md#api-keys) walks through it.

| Key | Default | Meaning |
| --- | --- | --- |
| `name` | required | Unique ID like a source ID (`[a-z0-9][a-z0-9_-]{0,62}`). It names the key in this file; the artifacts hold only its hash. |
| `key` | required | `{ env = "VAR" }`: the secret, `fyrr_key_` followed by at least 43 and at most 128 characters from `[A-Za-z0-9_-]`, at least 10 of them distinct. The shape does not prove randomness; generating the secret does: `echo "fyrr_key_$(openssl rand -hex 32)"`. Every key needs its own variable and value. |
| `readers` | `[]` | Reader tokens (`usr:<id>`, `grp:<id>`) the key acts with, as `stdio_readers`, including source-scoped tokens for `permissions = "sync"` sources. |
| `user` | absent | 1–512 bytes. Instead of `readers`: act as this canonical directory user (their corporate e-mail), with their groups, and so with whatever the sources grant that user; only a `readers` key has exactly the declared grants. Refused while the user is absent or inactive. |
| `expires` | absent | An RFC 3339 timestamp with an offset, such as `2027-06-30T00:00:00Z`, to the whole second (a fraction is dropped). The key is expired from that second on: the reader refuses it, and any request admitted before it, with no further step. A reader knows an expiry only from the published binding, so setting or changing one is published like any other identity change. |
| `requests_per_minute` | absent | At least 1. Replaces `reader.http.user_requests_per_minute` for this key; only with `readers`, because a `user` key shares that user's allowance. |

Set exactly one of `readers` (not empty) and `user`.

Only the **writer** reads the secret: it publishes a keyed hash, so the
reader and the reader-only build need no key in their environment. Every host
that publishes identity needs every key's variable: each `writer serve`, each
scheduled `writer commit`, and a separate `writer sync --directory` or
`writer directory` host. A host without one publishes identity without that
key's binding, so the key works only until that host publishes next. `config
check` treats the variable like any other referenced secret: it reports it
when it is unset, and `config check --no-env` (the way to check a reader
host's file) skips that. With the variable set it also refuses a secret of
the wrong shape and two keys with the same secret. `writer status` lists
`api_key_unbound` when a configured key is not admitted by the published
identity (for example its user is inactive), `api_key_expired` when its
`expires` has passed and `api_key_secret_unusable` when its variable is unset
or malformed: the writer then skips that key's binding (it fails closed) and
every other key and user is published as usual. Two keys that share a secret
stop the writer's commit instead.

A key added, removed or rebound by a published identity takes effect on a
running reader with its next version check
([`reader.version_check_interval`](reader-writer.md)), at the latest two
intervals (60 s by default) after the identity is published;
`POST /admin/refresh` applies it before it returns, and `SIGHUP` only starts
the check. A published `expires` needs no further step when its time comes:
the reader's clock refuses the key from that second. Setting or changing one
is an identity change and reaches readers like any other. For an urgent
revocation, remove the key on every host that publishes identity, publish
it, then send `POST /admin/refresh` to each reader; see
[API keys](security.md#api-keys).

`reader.http` needs `[identity.oidc]` or at least one API key only where the
HTTP reader starts: `reader serve` refuses to start without either, and
`config check` warns about it. The writer accepts the same file with no key,
so removing the last key still publishes (zero key bindings) and revokes it, on a running reader as for any other key.

### `[identity.directory]`

| Key | Default | Meaning |
| --- | --- | --- |
| `provider` | `none` | `none`, `snapshot`, `keycloak` or `scim`. |
| `sync_interval` | `15m` | Pull interval, or SCIM liveness interval renewed by any successful authenticated SCIM request. |
| `credentials` | absent | `{ env = "VAR" }`: a Keycloak admin bearer token, a Keycloak client secret when `client_id` is set, or the SCIM [provider](architecture.md#terms) credential. |
| `endpoint` | absent | Keycloak base URL (HTTPS, or explicitly allowed loopback). |
| `realm` | absent | Keycloak realm. |
| `client_id` | absent | Optional Keycloak service-account client. |
| `issuer` | absent | Issuer under which the directory binds OIDC subjects; required for SCIM. A Keycloak directory without it uses `<endpoint>/realms/<realm>`. |
| `verified_domains` | `[]` | Corporate e-mail domains accepted for canonical users; required for SCIM and Keycloak. Slack and Jira sources without `settings.corporate_domains` use the same list. |
| `snapshot_file` | absent | A complete directory snapshot file for `provider = "snapshot"`. |

Only a completed directory read advances directory freshness. Ordinary commits
and `writer directory --snapshot` imports do not. With `provider = "none"`,
imported snapshots remain usable without a directory freshness timestamp, and
an import under `provider = "none"` withdraws a timestamp that an earlier
provider left, so a deployment that moves from `keycloak`, `scim` or
`snapshot` to manual imports needs only one import to be served again. The
import publishes its identity and the withdrawal in one head write. The
imported snapshot must be built after the last provider pull (its `built_at`
not older than that pull's): an older one is refused as stale
(`directory_snapshot_stale`) and the timestamp stays. With a provider
configured, an import keeps the timestamp the provider attested.
Once a head carries a timestamp, readers enforce `validity_deadline` on it
even if their own `provider` is `none`: the publication, not the reader's
file, says whether a directory clock exists.

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
pulls publish no directory snapshot or freshness proof.

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
