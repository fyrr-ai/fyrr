# HTTP login and users

> **In short**
>
> - `fyrr reader serve` can serve many users over Streamable HTTP at `/mcp`;
>   each request carries the user's OIDC access token or an API key.
> - [API keys](#api-keys) need no identity provider: one key per agent role
>   or per person, each with the groups it may read.
> - With Keycloak it takes two Keycloak objects, about ten configuration
>   keys, a writer that pulls the users and a reader behind a TLS proxy.
> - The [reference](#reference) at the end lists the exact token rules and
>   limits, also for identity providers other than Keycloak.

## What you get

- Every user connects their own MCP client to one URL and signs in with the
  identity [provider](architecture.md#terms); fyrr serves each of them only what they may read.
- Or a client presents an operator-issued API key instead, with the grants
  the operator declared for it.
- Users, groups and deactivations come from the directory on a schedule; a
  deactivated user is refused at the next published pull.
- One reader process serves everyone; readers and writers share only S3.

The rules behind it (subjects, reader tokens, the canonical directory) are
in the [security model](security.md).

## API keys

Use an API key when a client cannot sign in through an identity provider:
an unattended coding agent, or a small team that wants to share one reader
without running Keycloak. A key is a secret the operator hands out, and the
operator states in the configuration what it may read.

1. **Generate a key.** Make one per agent role or per person, and keep it
   in your secret store:

   ```sh
   echo "fyrr_key_$(openssl rand -hex 32)"
   ```

2. **Declare it.** On the writer host, export the key (for example as
   `FYRR_API_KEY_BUGFIX`) and add a table to the
   [configuration file](configuration.md), next to `[reader.http]`:

   ```toml
   [identity]
   hashing_key = { env = "FYRR_IDENTITY_KEY" }

   [[identity.api_keys]]
   name = "bugfix-agents"
   key = { env = "FYRR_API_KEY_BUGFIX" }
   readers = ["grp:engineering"]     # what this key may read, as for stdio_readers

   [reader.http]
   listen = "127.0.0.1:8080"
   public_url = "https://fyrr.example.com/mcp"
   ```

   Without `[identity.oidc]` the reader accepts only keys. A key may carry
   `expires` and `requests_per_minute` too; see
   [`[[identity.api_keys]]`](identity.md#identityapi_keys).
3. **Commit the change.** The writer publishes only a keyed hash of the
   key. `fyrr writer serve` does it at its next commit (60 seconds by
   default) once it runs with the new configuration, which means after a
   restart; otherwise run `fyrr writer commit`. The reader hosts do not need
   the key, but **every host that publishes identity does**: every
   `fyrr writer serve`, every scheduled `fyrr writer commit`, and a separate
   host that runs `fyrr writer sync --directory` or `fyrr writer directory`.
   Each needs every key's variable in its environment. A host without one
   publishes identity without that key's binding (`api_key_secret_unusable`),
   so the key works only until that host publishes next, which looks like an
   intermittent `401`. A running reader accepts a new key with its next
   version check, at the latest two `reader.version_check_interval`s (60 s
   by default) after the identity is published, and a request in the
   meantime gets `401`; `POST /admin/refresh` runs the check and returns
   when it has completed, and `SIGHUP` starts one without waiting for it. If keys are configured but none is
   usable, the reader starts and reports ready while every key gets `401`;
   `fyrr writer status` shows why.
4. **Start the reader** behind a TLS-terminating proxy, as in
   [step 3](#3-start-the-writer-and-the-reader). A key travels in every
   request: never serve it over plain HTTP outside loopback.
5. **Connect a client.** Give the client the *name* of an environment
   variable that holds the key, never the key itself, so that neither the
   process arguments nor the client's stored settings contain the secret.
   Claude Code expands `${VAR}` in a header when it starts; use single
   quotes so that your shell leaves it alone:

   ```sh
   claude mcp add --transport http fyrr https://fyrr.example.com/mcp \
     --header 'Authorization: Bearer ${FYRR_API_KEY_BUGFIX}'
   ```

   Claude Code stores the text `${FYRR_API_KEY_BUGFIX}` in its settings and
   reads the variable from the environment it was started in, so start it
   from a shell that has the variable exported. If the variable is missing
   Claude Code warns and sends the literal text, which fyrr refuses with
   `401`. The same `${VAR}` form works in the `headers` of a `.mcp.json`.
   Codex reads the bearer token from an environment variable that is set
   when it starts, and stores only the variable's name:

   ```sh
   codex mcp add fyrr --url https://fyrr.example.com/mcp \
     --bearer-token-env-var FYRR_API_KEY_BUGFIX
   ```

   Any other client needs one header, `Authorization: Bearer <key>`; prefer
   a client setting that reads it from the environment or a secret store.

A key has two forms. `readers` gives it reader tokens of its own, exactly as
`stdio_readers` does for the stdio subject, so it sees what those groups
and users may read and nothing else: exactly what the operator declared.
`user = "ada@example.com"` makes it act as that
[canonical directory user](architecture.md#terms) instead, with their
groups; it needs a directory ([Keycloak](#1-prepare-keycloak) or another
provider) and is refused while that user is missing or inactive. Because it
acts as that user, it also gains whatever the sources grant them, now or
later: their document permissions and their groups' are not limited to what
the configuration says.

To rotate, add a second key, move the client to it, then remove the first.
To revoke, remove the table on **every host that publishes identity** (each
`writer serve` and each scheduled `writer commit`) and restart or rerun
them: a writer that still runs with the old file republishes the key. A
running reader refuses the removed key with its next version check, at the
latest two intervals (60 s by default) after the identity is published. For
an urgent revocation, publish (`fyrr writer commit`, or restart `writer
serve`) and then send `POST /admin/refresh` to each reader (it needs
`reader.http.operator_credential`): it returns after the check has completed,
so the key is refused from the next request on. `SIGHUP` only starts a check
and does not wait for it, so it is not a confirmation. A reader learns an
`expires` only from the published binding: once published, it needs no
further step when its time comes, but setting or changing one is an identity
change, published and applied like any other. See
[API keys in the security model](security.md#api-keys).
Removing the last key is fine: the writer then publishes no key bindings,
and a reader restarted without any key or `identity.oidc` refuses to start.
A refused key always gets the same `401 unauthorized`, whether it is unknown,
malformed, removed or expired.

## Sign-in with Keycloak

The rest of this page sets up users who sign in through Keycloak.

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
`WWW-Authenticate`. Without `[identity.oidc]` there is no authorization
server: that route answers 404 and a 401 carries a bare `WWW-Authenticate:
Bearer`, so a client has no sign-in to start and needs its key configured. To check by hand with a user's access token:

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
| `503 authorization_unavailable` | The token is valid but its subject is not bound: the user is not yet in a published directory pull, is disabled, or has no verified e-mail in `verified_domains`. It also means the last directory pull is older than `identity.validity_deadline`, or the keys could not be fetched. Check `fyrr writer status`. |
| `401 unauthorized` with an API key | The key is unknown to the published identity: not committed yet (run `fyrr writer commit`; a running reader applies a new identity with its next version check, within two intervals, or at once after `POST /admin/refresh`), removed, past its `expires`, malformed (`fyrr_key_` plus 43–128 characters of `[A-Za-z0-9_-]`, at least 10 of them distinct) or, for a `user` key, its user is missing or inactive (`fyrr writer status` warns `api_key_unbound`; `api_key_secret_unusable` means the variable is unset or malformed on a host that published identity, so that publication did not bind the key; every host that publishes identity needs it, or the key works intermittently). Claude Code started without the variable sends the literal `${VAR}` text. The reader says nothing more, by design. |
| A user is missing although the e-mail looks right | Keycloak does not mark it verified. Users federated from LDAP need *Trust Email* on the LDAP provider, or a verified flag set by an administrator. |
| `403 host_mismatch` | The proxy does not pass the public `Host`: set `proxy_set_header Host $host`. The reader logs `http refused code=host_mismatch`. |
| `403 invalid_argument` | The request had an `Origin` header: browsers cannot call `/mcp` directly. |
| `config check` reports `identity.oidc.issuer` | The configured issuer differs from the directory's; leave `issuer` out to take the directory's. |

## Reference

### Tokens

A request is served only when every rule holds. Invalid tokens receive
`401 unauthorized` with a `WWW-Authenticate` header; unavailable keys or
subject bindings return `503 authorization_unavailable`. A bearer that
starts with `fyrr_key_` is an [API key](#api-keys) and takes its own path
(below); everything else is an OIDC access token, and without
`[identity.oidc]` it is refused with 401.

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
  authorization_unavailable`. The same answer follows when the serving head
  carries a directory freshness timestamp older than
  `identity.validity_deadline`, whatever the reader's own
  `identity.directory.provider`.

### API key rules

- The bearer is `fyrr_key_` followed by 43–128 characters from
  `[A-Za-z0-9_-]`, at least 10 of them distinct; anything else that starts
  with `fyrr_key_` is refused before it is hashed.
- Its keyed hash must be bound in the identity of the serving head the
  reader holds (the one its latest version check read, as for OIDC subjects),
  and its binding must not have expired. Otherwise the request gets `401
  unauthorized`, the same for an unknown, malformed, removed and expired key,
  with no storage read: the lookup comes before the namespace and directory
  checks, so only a bound key can get `503` for them. A key added, removed or
  rebound takes effect with the reader's next version check, at the latest two
  intervals after the identity is published; if the held head is older than
  that and the reader cannot read it, every request gets the same `503`.
- A binding's `expires` is whole seconds. The key is expired from the second
  it names: a request is refused when the reader's clock, truncated to whole
  seconds, is at or after it, and the check runs again at the HTTP boundary,
  once the response is built and before any of it is returned, so a request
  admitted just before `expires` returns no result after it (bytes already
  handed to the socket are out of the reader's hands; see
  [security](security.md#api-keys)).
- A bound key then goes through the same admission as an OIDC subject:
  directory freshness, `identity.max_tokens_per_user` and the request
  allowances. A key with `readers` is no group member, so only its own
  allowance applies (its `requests_per_minute` when the reader's configuration
  lists it, else `reader.http.user_requests_per_minute`) and
  `group_requests_per_minute` does not. A `user` key shares that user's
  allowance and their groups'.

### JWKS keys

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

### Other directories

Use a [SCIM or snapshot directory](identity.md#other-directory-providers) to
bind subjects when not using Keycloak.

The writer's administration listener serves `/admin/status` and the SCIM
endpoints with two separate credentials; a non-loopback listener requires
TLS termination in front of it, declared with `tls_terminated = true`. All
keys are in [identity](identity.md) and
[reader settings](reader-writer.md#readerhttp).

A signing key withdrawn by the identity provider remains trusted for up to
`jwks_validity` after the last successful JWKS fetch. Stale keys fail closed
when refresh fails. Unknown key IDs trigger at most one refresh attempt per
`jwks_refresh_interval`; an in-flight refresh does not block current keys.
