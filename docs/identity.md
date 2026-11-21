# Identity and access configuration

> **In short**
>
> - The `[identity]` table: the stdio subject, the hashing and encryption
>   keys, and how long permission and directory reads stay valid.
> - `[identity.directory]` selects where [canonical users](architecture.md#terms) and groups come from;
>   `[identity.oidc]` holds the OIDC settings for HTTP MCP.
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
| `directory` | see below | Canonical directory. |

`validity_deadline` must cover the longest permission/member interval or
`identity.directory.sync_interval` (even with `provider = "none"`), plus
`writer.commit.coalescing_timer`, a five-minute margin and
`reader.version_check_interval`. `config check` prints
the required value.

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
