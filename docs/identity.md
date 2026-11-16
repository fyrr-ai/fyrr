# Identity and access configuration

> **In short**
>
> - The `[identity]` table: the stdio subject, the hashing and encryption
>   keys, and how long permission and directory reads stay valid.
> - `[identity.directory]` selects where canonical users and groups come from;
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
| `hashing_key` | required | Namespace HMAC key, `{ env = "VAR" }`; the same value on every writer and reader. Only hashed reader tokens and principal keys enter the published artifacts and rows. |
| `encryption_key` | absent | 32 random bytes as 64 hexadecimal characters, `{ env = "VAR" }`; a separate secret from `hashing_key`. Encrypts persisted identity inputs (directory snapshots, membership, SCIM state, stored credentials). `config check` requires it with a `permissions = "sync"` source, a `snapshot` or `keycloak` directory, or writer administration. |
| `validity_deadline` | `1h` | How long the last successful permission and member read of a `permissions = "sync"` source, and the last directory read, stay valid. After it the source is hidden (`metadata.hidden_sources`); an expired directory read refuses the whole namespace. No directory clock applies when `provider = "none"`. |
| `max_tokens_per_user` | `5000` | At least 1. A subject with more reader tokens gets `authorization_unavailable`; other subjects are unaffected. |
| `oidc` | absent | OIDC settings for HTTP MCP, see below. |
| `directory` | see below | Canonical directory. |

`validity_deadline` must be at least the longest permission, member or
directory interval plus `writer.commit.coalescing_timer`, the reader version
check interval and a five-minute margin; `config check` prints the required
value.

### `[identity.directory]`

| Key | Default | Meaning |
| --- | --- | --- |
| `provider` | `none` | `none`, `snapshot`, `keycloak` or `scim`. |
| `sync_interval` | `15m` | Pull interval, or SCIM liveness interval renewed by any successful authenticated SCIM request. |
| `credentials` | absent | `{ env = "VAR" }`: a Keycloak admin bearer token, a Keycloak client secret when `client_id` is set, or the SCIM provider credential. |
| `endpoint` | absent | Keycloak base URL (HTTPS, or explicitly allowed loopback). |
| `realm` | absent | Keycloak realm. |
| `client_id` | absent | Optional Keycloak service-account client. |
| `issuer` | absent | Issuer under which the directory binds OIDC subjects; required for SCIM. A Keycloak directory without it uses `<endpoint>/realms/<realm>`. |
| `verified_domains` | `[]` | Corporate e-mail domains accepted for canonical users; required for SCIM and Keycloak. Slack and Jira sources without `settings.corporate_domains` use the same list. |
| `snapshot_file` | absent | A complete directory snapshot file for `provider = "snapshot"`. |

Only a completed directory read advances directory freshness. Ordinary commits
and `writer directory --snapshot` imports do not. With `provider = "none"`,
imported snapshots remain usable without a directory freshness timestamp.

Keycloak directory snapshots are bounded to 100,000 users, 100,000 groups
and 1,000,000 combined user/group membership edges. Each user needs two
membership count checks and a paged membership read, plus directory and
group scans. The whole pull uses `--timeout` (default `10m`).

A pull uses one access token and does not refresh it mid-read. Its remaining
lifetime must exceed the read duration; increasing `--timeout` does not
extend that lifetime. With `client_id`, configure the service-account
client's token lifespan in Keycloak for the expected read duration. The
short-lived admin-token example suits small realms. Partial or failed
pulls publish no directory snapshot or freshness proof.

### `[identity.oidc]`

| Key | Default | Meaning |
| --- | --- | --- |
| `issuer` | required | Exact `iss` of accepted tokens. With a `keycloak` directory it defaults to the directory issuer. With a `keycloak` or `scim` directory it must equal that issuer, which binds every subject; `config check` refuses a mismatch. |
| `audience` | required | The token's `aud`, a string or an array, must contain it. |
| `jwks_url` | required | Operator-pinned key endpoint; token headers never choose a URL. With `keycloak = true` it defaults to `<issuer>/protocol/openid-connect/certs`. |
| `jwks_validity` | `5m` | |
| `jwks_refresh_interval` | `5s` | |
| `scope` | `fyrr:read` | Required scope. |
| `keycloak` | `false` | Accept Keycloak's `JWT`/`Bearer` access-token profile instead of `at+jwt`. On by default when `issuer` comes from a Keycloak directory. |

The derived values come from the configuration file only, never from a
token, and an explicit key always wins. `fyrr config show` prints the
effective values.
