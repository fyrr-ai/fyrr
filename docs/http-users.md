# HTTP login and users

> **In short**
>
> - `fyrr reader serve` can serve many users over Streamable HTTP at `/mcp`;
>   each request carries the user's OIDC access token or an
>   [API key](#api-keys).
> - With Keycloak it takes two Keycloak objects, about ten configuration
>   keys, a writer that pulls the users and a reader behind a TLS proxy.
> - The [reference](#reference) at the end lists the exact token rules and
>   limits, also for identity providers other than Keycloak.

## What you get

- Every user connects their own MCP client to one URL and signs in with the
  identity [provider](architecture.md#terms); fyrr serves each of them only what they may read.
- Users, groups and deactivations come from the directory on a schedule; a
  deactivated user is refused at the next published pull.
- One reader process serves everyone; readers and writers share only S3.

The rules behind it (subjects, reader tokens, the canonical directory) are
in the [security model](security.md).

## API keys

An API key needs no identity provider: it is a secret you issue to an agent
role, with the reader tokens that role may read. Use one key per role.

1. Generate the secret and keep it in your secret store:

   ```sh
   echo "fyrr_key_$(openssl rand -hex 32)"
   ```

2. On every host that publishes identity (each `writer serve` and scheduled
   `writer commit`), export the secret as an environment variable and add the
   key to the [configuration file](identity.md#identityapi_keys):

   ```toml
   [[identity.api_keys]]
   name = "bugfix-agents"                    # unique ID
   key = { env = "FYRR_API_KEY_BUGFIX" }
   readers = ["grp:engineering"]             # as identity.stdio_readers

   [reader.http]
   listen = "127.0.0.1:8080"
   public_url = "https://fyrr.example.com/mcp"
   ```

3. Publish with `fyrr writer commit` (or let `writer serve` do it), then
   publish the keys with `fyrr writer keys` (a required step: readers refuse
   every API key until it has run), and start
   `fyrr reader serve` behind a TLS-terminating proxy as in
   [step 3](#3-start-the-writer-and-the-reader). The reader needs the
   namespace `hashing_key`, but no key's secret variable; on a reader host,
   `fyrr config check` reports those variables as absent unless you run it
   with `--no-env`. Without TLS the secret crosses the network in clear text:
   do not expose the listener directly.

4. Connect a client with the secret in its environment:

   ```sh
   claude mcp add --transport http fyrr https://fyrr.example.com/mcp \
     --header 'Authorization: Bearer ${FYRR_API_KEY_BUGFIX}'
   codex mcp add fyrr --url https://fyrr.example.com/mcp \
     --bearer-token-env-var FYRR_API_KEY_BUGFIX
   ```

   The single quotes keep the literal `${FYRR_API_KEY_BUGFIX}` in what Claude
   Code stores (`~/.claude.json` at the default scope, `.mcp.json` with
   `--scope project`) and it expands the variable when it connects; the
   shell must not expand it.

To revoke a key:

1. Remove it from the configuration on every host that publishes identity.
2. Restart every running `writer serve` there, and let scheduled `writer
   commit` jobs pick up the file. A running `writer serve` keeps the
   configuration it started with and republishes a removed key at its next
   commit.
3. Publish: run `fyrr writer keys`, which is what readers check first, and
   `fyrr writer commit` (or wait for the restarted service's next commit,
   after `writer.commit.coalescing_timer`, default 60 s, unless
   `writer.serve.committer = false`) to drop the key's grants. A commit
   rebuilds the identity from the configuration even when no content changed.
4. Send `POST /admin/refresh` to each reader; without that, a reader refuses
   the key within two `reader.version_check_interval`s of `writer keys`.

[Identity](identity.md#identityapi_keys) describes the roles and the
namespace prefix a key can carry.

A reader that holds a usable head and identity answers an unknown, malformed
or removed key with `401`, and without OIDC the challenge is a bare
`WWW-Authenticate: Bearer`. When the reader cannot read the head or the
identity, every caller gets `503 authorization_unavailable`. A key with a
missing or malformed secret is left out of the identity by `writer commit`,
which publishes the other keys as usual, and fails `writer keys`. See [security](security.md#api-keys) for
what a key grants.

With `[identity.oidc]` as well, the reader accepts both: a bearer that
starts with `fyrr_key_` is an API key, anything else an OIDC access token.
A reader takes the key path only when its own configuration declares at least
one `[[identity.api_keys]]`; otherwise a `fyrr_key_` bearer is an OIDC token
and is refused as one.

## 1. Prepare Keycloak

In the realm your users sign in to, create two objects:

1. **A directory client, `fyrr-directory`.** A confidential OpenID Connect
   client with *Client authentication* on and *Service accounts roles*
   enabled (no other flows). Under *Service account roles*, assign the
   `view-users` role of the `realm-management` client. Copy its client
   secret from *Credentials*.
2. **A client scope, `fyrr:read`.** OpenID Connect, *Include in token
   scope* on. Under *Mappers*, add an *Audience* mapper with *Included
   Custom Audience* `fyrr` and *Add to access token* on. Then open the
   client your users sign in with (the one their MCP client uses) and add
   `fyrr:read` under *Client scopes* as a **Default** scope. Its access tokens then carry `fyrr` in `aud` and
   `fyrr:read` in `scope`; other audiences, such as `account`, may stay.

Users need an e-mail address that Keycloak marks as verified, in one of your
corporate domains.

## 2. Configure fyrr

Add these keys to the [configuration file](configuration.md):

```toml
[identity]
hashing_key = { env = "FYRR_IDENTITY_KEY" }        # the same on every host
encryption_key = { env = "FYRR_ENCRYPTION_KEY" }   # 64 hexadecimal characters

[identity.directory]
provider = "keycloak"
endpoint = "https://sso.example.com"
realm = "acme"
client_id = "fyrr-directory"
credentials = { env = "FYRR_DIRECTORY_CLIENT_SECRET" }
verified_domains = ["example.com"]

[identity.oidc]
audience = "fyrr"

[reader.http]
listen = "127.0.0.1:8080"
public_url = "https://fyrr.example.com/mcp"
```

Nothing else is needed: the token issuer defaults to the realm
(`https://sso.example.com/realms/acme`), the Keycloak token profile is on
and the keys come from the realm's `protocol/openid-connect/certs`, all from
this file, never from a token. Then check the file:

```sh
fyrr config check
fyrr config show | grep identity.oidc    # the derived issuer and key URL
```

## 3. Start the writer and the reader

```sh
fyrr writer namespace create      # once
fyrr writer serve                 # keeps running: sources and the directory on schedule
```

`writer serve` pulls the directory every `identity.directory.sync_interval`
(15 minutes) and publishes changes by the next successful pull and commit;
a membership change made during a pull may wait for the following pull.
[Scheduling](deployment.md#scheduling) shows the cron alternative. Every enabled
user with a verified e-mail in `verified_domains` becomes a
[canonical user](architecture.md#terms).

Start the reader on the reader host, behind a TLS-terminating proxy that
keeps the public `Host` header:

```sh
fyrr reader serve                 # plain HTTP on reader.http.listen
```

```nginx
server {
    listen 443 ssl;
    server_name fyrr.example.com;
    # ssl_certificate and ssl_certificate_key as usual
    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_buffering off;
        proxy_read_timeout 310s;
    }
}
```

Clients must not reach the listener directly.

## 4. Grant access

Each canonical user holds the reader token `usr:<lower-cased e-mail>` and
`grp:keycloak:<group ID>` for each Keycloak group they belong to (the group
ID is the UUID in the group's Keycloak URL). List them in a fixed source:

```toml
[[sources]]
id = "handbook"
type = "local-files"
permissions = "fixed"
readers = ["grp:keycloak:11111111-2222-3333-4444-555555555555", "usr:ada@example.com"]
[sources.scope]
paths = ["/srv/handbook"]
```

`permissions = "sync"` sources link their accounts to canonical users by
verified corporate e-mail: Google Drive (its Workspace domain), Slack and
Jira do this without extra work (Slack and Jira use `verified_domains`
unless their `corporate_domains` is set). Notion and local files are always
`fixed`. GitHub reports no usable e-mail, so its
accounts need explicit links in a directory snapshot; with HTTP users, use
GitHub with `permissions = "fixed"`.

## 5. Connect an MCP client

Point the client at `https://fyrr.example.com/mcp`. Clients that discover
OAuth settings read the issuer and scope from
`/.well-known/oauth-protected-resource`; a 401 carries the same pointer in
`WWW-Authenticate`. To check by hand with a user's access token:

```sh
curl https://fyrr.example.com/mcp \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Accept: application/json, text/event-stream" \
  -H "Content-Type: application/json" \
  -H "MCP-Protocol-Version: 2025-11-25" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":{"query":"backup"}}}'
```

## 6. Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `401 unauthorized` | The token failed a [token rule](#tokens): usually the audience (`fyrr:read` is not a default scope of the sign-in client, or its Audience mapper does not add `fyrr` to the access token), an expired token, or a token from another realm. Decode it and compare `iss`, `aud`, `scope` and `typ`. |
| `503 authorization_unavailable` | The token is valid but its subject is not bound: the user is not yet in a published directory pull, is disabled, or has no verified e-mail in `verified_domains`. It also means the keys could not be fetched. Check `fyrr writer status`. |
| A user is missing although the e-mail looks right | Keycloak does not mark it verified. Users federated from LDAP need *Trust Email* on the LDAP provider, or a verified flag set by an administrator. |
| `403 host_mismatch` | The proxy does not pass the public `Host`: set `proxy_set_header Host $host`. The reader logs `http refused code=host_mismatch`. |
| `403 invalid_argument` | The request had an `Origin` header: browsers cannot call `/mcp` directly. |
| `config check` reports `identity.oidc.issuer` | The configured issuer differs from the directory's; leave `issuer` out to take the directory's. |

## Reference

### Tokens

A request is served only when every rule holds. Invalid tokens receive
`401 unauthorized` with a `WWW-Authenticate` header; unavailable keys or
subject bindings return `503 authorization_unavailable`.

- At most 16 KiB, signed with RS256 by a key from `identity.oidc.jwks_url`
  whose `kid` is in the header. The key URL is pinned by the operator;
  header fields such as `jku` are ignored.
- Header `typ` is `at+jwt` (RFC 9068), or `JWT` with claim `typ = "Bearer"`
  when `identity.oidc.keycloak = true`.
- `iss` equals `identity.oidc.issuer`; `aud`, a string or an array,
  contains `identity.oidc.audience`; the space-separated `scope` contains
  `identity.oidc.scope` (`fyrr:read`).
- `exp` is in the future and `nbf`, when present, in the past, with no
  leeway; `sub` has 1–512 bytes.
- The subject (`iss` and `sub`) must be bound to an active canonical user in
  the published identity, or the request gets `503
  authorization_unavailable`.

### Keys

The JWKS document may hold at most 16 keys with distinct `kid`s in at most
256 KiB; usable keys are RSA with 2048–4096-bit moduli and exponent 65537.
It is fetched without redirects within 3 seconds, at most every
`jwks_refresh_interval` (5 s, also on an unknown `kid`), and trusted for
`jwks_validity` (5 minutes); after that, without a successful fetch,
requests get `503 authorization_unavailable`.

### HTTP boundary

- The `Host` header must equal the authority of `public_url` on every route
  except `/readyz` (`403 host_mismatch`). Requests with an `Origin` header
  are refused on every route.
- Bodies are limited to 1 MiB, headers to 64 and 32 KiB, connections to 64
  and a request to 301 seconds. Each canonical user and each group has
  `user_requests_per_minute` and `group_requests_per_minute` (120); over
  them, `429 rate_limited`.
- `GET /readyz` is 200 when the reader can serve, else 503. With the public
  `Host` the body names the serving publication; with any other `Host`, as
  health probes send it, the body is empty.
- `POST /admin/refresh` forces a head check, at most every 5 seconds, with
  `Authorization: Bearer` and the value of `reader.http.operator_credential`;
  without that key the route does not exist (404). Readers pick up new
  versions on their own every `reader.version_check_interval`.
- `POST /v1/namespaces/{namespace}/query` and `.../fetch` are the
  [HTTP JSON API](http-api.md) for `search` and `fetch`; they pass the same
  checks as `/mcp` and serve the namespace the path names.

### Other directories

Use a [SCIM or snapshot directory](identity.md#other-directory-providers) to
bind subjects when not using Keycloak.

The writer's administration listener serves `/admin/status` and the SCIM
endpoints with two separate credentials, and `DELETE /v1/namespaces/{namespace}`
to an API key with the `admin` role; a non-loopback listener requires
TLS termination in front of it, declared with `tls_terminated = true`. All
keys are in [identity](identity.md) and
[reader settings](reader-writer.md#readerhttp).

A signing key withdrawn by the identity provider remains trusted for up to
`jwks_validity` after the last successful JWKS fetch. Stale keys fail closed
when refresh fails. Unknown key IDs trigger at most one refresh attempt per
`jwks_refresh_interval`; an in-flight refresh does not block current keys.
