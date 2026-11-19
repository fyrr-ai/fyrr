# Configuration

> **In short**
>
> - One TOML file holds the whole deployment; this page covers the file,
>   secrets, `[storage]`, `[embedding]` and `[[sources]]`.
> - Most deployments set only these tables and `[identity]`, which has
>   [its own page](identity.md).
> - `[namespace]` is in [tuning](tuning.md); `[reader]` and `[writer]` are in
>   [reader and writer settings](reader-writer.md).

`fyrr` reads `fyrr.toml` in the
working directory, `--config PATH` or `FYRR_CONFIG`. Unknown keys are
rejected, and error messages name the key path without quoting a value.
`schemas/config.schema.json` is the generated JSON Schema for editors;
`fyrr config check`, `config show` and `config schema` are described in the
[CLI reference](cli.md#fyrr-config). Most deployments set only `[storage]`,
`[embedding]`, `[identity]` and `[[sources]]`; the keys under
[Advanced tuning](tuning.md#advanced-tuning) are best left at their defaults.

The default `[namespace.retrieval]` policy gives the dense lane weight `0.5`
and retains `200` lexical candidates and `75` dense candidates per query.
Dense rescoring stays in `legacy` mode. These settings enter the published
retrieval-policy digest; see [upgrades](operations.md#upgrades) when changing
an existing deployment from the previous defaults.

## Secrets

A secret is always a reference to an environment variable:

```toml
secret_access_key = { env = "FYRR_S3_SECRET_ACCESS_KEY" }
```

A literal in a secret field is refused. So is a string under a secret-like
key (`token`, `password`, `secret`, `api_key`, `credential`, …) anywhere in
a source's `settings` or `scope`, including inside arrays. Provider schemas
mark their own secret fields (`x-fyrr-secret`); those accept only an
environment reference too. Variable names match `^[A-Z_][A-Z0-9_]*$`.

Durations and byte sizes are TOML strings. A duration is an integer with a
unit: `"500ms"`, `"30s"`, `"15m"`, `"1h"`, `"1d"`. A byte size is an integer
with `B`, `KiB`, `MiB`, `GiB` or `TiB`, or without a unit for bytes
(`"4096"`); a bare TOML integer is refused. Keys documented as an integer,
such as `query_cache_max_bytes`, take a TOML integer instead.

## `[storage]`

| Key | Default | Meaning |
| --- | --- | --- |
| `uri` | required | `s3://bucket[/prefix]` for S3 or an S3-compatible service, or `file:///absolute/path` for a directory on this host ([local storage](#local-storage)). |
| `region` | `us-east-1` | |
| `endpoint` | absent | Endpoint of an S3-compatible service; absent uses `AWS_ENDPOINT_URL` when set, otherwise AWS S3. |
| `allow_http` | `false` | Allow a plain-HTTP endpoint (development only). |
| `virtual_hosted_style` | `false` | Virtual-hosted-style requests instead of path-style. |
| `access_key_id`, `secret_access_key`, `session_token` | absent | Set `access_key_id` and `secret_access_key` together; `session_token` requires both. Otherwise uses the environment and role sources below. |

All keys but `uri` configure S3 access; with a `file://` URI, `config check`
refuses each of them, also one that repeats its default.

S3 uses explicit credential references or `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` and optional `AWS_SESSION_TOKEN`. Without static
credentials, it tries web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` and
`AWS_ROLE_ARN`), container role credentials, then an EC2 instance profile.

Set `AWS_EC2_METADATA_DISABLED=true` on a non-EC2 host when no role credentials
are available. It disables only the EC2 metadata fallback and reports
`storage_unavailable` immediately instead of waiting for metadata discovery.
Explicit credentials, web identity and ECS/EKS container credentials remain
enabled. Leave it unset on EC2 when using the instance's role.

### Local storage

```toml
[storage]
uri = "file:///var/lib/fyrr"
```

Use local storage for one host and user:

- Writers and readers run as the same user. The root must be private,
  owned by that user and have no symlink ancestors; new roots use mode 0700.
- Use a local filesystem with user extended attributes and `fsync`. NFS,
  SMB/CIFS, AFS, Ceph and 9p are detected and refused. FUSE, virtiofs,
  Lustre and other remote or synchronized filesystems remain unsupported.
- The URI must be absolute, without a host, query, `.`, `..` or empty
  segments; percent-encode reserved characters (`file:///srv/my%20data`).
- Back up the whole root with its extended attributes and coordination file;
  never edit its objects. To move between local storage and S3, sync into a
  fresh namespace on the other storage.

See [local storage mechanics](architecture.md#local-storage) for coordination,
cleanup and mirror behavior. The configuration schema is revision `5.1`.

## `[embedding]`

The [provider](architecture.md#terms) must implement the OpenAI-compatible embeddings protocol.

| Key | Default | Meaning |
| --- | --- | --- |
| `endpoint` | required | Base URL of the embeddings API (`…/v1`). |
| `model` | `BAAI/bge-m3` | 1–512 UTF-8 bytes. |
| `dimensions` | `1024` | 1–8192; must match the model. |
| `send_dimensions` | `false` | Send `dimensions` in the request. |
| `prefix` | `""` | At most 4096 UTF-8 bytes, applied to documents and queries alike. |
| `roles.document.prefix`, `roles.query.prefix` | absent | Role-specific prefix of at most 4096 UTF-8 bytes, after the shared one. |
| `roles.document.input_type`, `roles.query.input_type` | absent | 1–128 UTF-8 bytes, sent as `input_type` only when configured. |
| `api_key` | absent | `{ env = "VAR" }`. |
| `batch_size` | `32` | 1–2048 documents per request. |
| `timeout` | `30s` | Per request; must be greater than zero. |
| `max_retries` | `3` | 0–10. |
| `reader_live` | `false` | Permit readers to embed queries after authorization. Off, the dense lane is degraded for every query. |
| `spend` | absent | Required for live calls: `limit_microusd` and `max_call_microusd` (1 to `limit_microusd`), in millionths of a US dollar. The scope is one reader process, one `writer sync` invocation (split evenly across its sources) or one source run under `writer serve`, including retries; it is not an account-wide cap. |
| `local_cache` | absent (`$XDG_CACHE_HOME/fyrr/writer-embeddings`) | Writer document-vector cache directory. |
| `query_cache_max_bytes` | `16777216` | 0–134217728 (128 MiB): bounded in-process query vector cache per reader process, in bytes. An integer; unit suffixes are refused. |

The writer declares its spend before every call and reserves
`max_call_microusd` even for an ambiguous timeout. The model, dimensions, prefixes,
roles and `send_dimensions` bind the namespace:
a change needs a fresh namespace ([upgrades](operations.md#upgrades)).

## `[[sources]]`

| Key | Default | Meaning |
| --- | --- | --- |
| `id` | required | Stable for the life of the source: lowercase letters, digits, `_` and `-`, at most 63 bytes. |
| `type` | required | Provider name, for example `local-files`, `google-drive`, `notion`, `github`, `slack`, `jira`. |
| `permissions` | `sync` | `sync` maps the source's own permissions; `fixed` grants the listed `readers` to every document. |
| `readers` | `[]` | Reader tokens of a fixed connection; at least one. Refused under `sync`. |
| `intervals.content` | `15m` | |
| `intervals.permissions` | absent | Required for `sync`, refused for `fixed`. |
| `intervals.members` | absent | Membership snapshot interval; refused for `fixed`. |
| `intervals.reconcile` | `1d` | Full-ID listing interval. |
| `document_max_bytes` | absent (`writer.document_max_bytes`) | Per-source override. |
| `row_warning`, `row_error` | absent | Row-count thresholds per sync; `row_warning` must be below `row_error`. |
| `retention_days` | absent | At least 1: retention of incoming documents. |
| `settings` | `{}` | Provider connection settings, validated by the provider's schema. |
| `scope` | `{}` | What to sync, validated by the provider's schema. |

A fixed connection makes no permission or member reads; `config check` warns
that every listed reader may read every document. Providers that support
only one mode (Notion and local files are always `fixed`) refuse the other.
The [connector pages](connectors/README.md) list each provider's `settings`
and `scope`.

## Example

`config/fyrr.example.toml` is a configuration-validation example: it shows
the shape of a deployment with a fixed and a synced source, and the test
suite checks that it passes `config check --no-env`. It omits keys a running
deployment may need, such as `[embedding.spend]`;
[minimal production setup](deployment.md#minimal-production-setup) shows a
configuration that runs.

`config check` refuses a file that would fail only at run time: without
`identity.hashing_key`, or without `identity.encryption_key` while a
`permissions = "sync"` source exists ([identity](identity.md#identity)).

```sh
fyrr --config config/fyrr.example.toml config check --no-env
```
