# CLI reference

> **In short**
>
> - Reference for every `fyrr config`, `fyrr reader` and `fyrr writer`
>   command, with flags, defaults and exit statuses.
> - The reader-only build has `reader` and `config` only.
> - Look a command up here; the task-oriented pages are
>   [deployment](deployment.md) and [operations](operations.md).

```
fyrr [OPTIONS] <COMMAND>
```

The only global option, `--config` (or `FYRR_CONFIG`), names the
configuration file; the default is `fyrr.toml`. `fyrr --version` prints the
version and the build (`full build` or `reader-only build`). Every
subcommand accepts `--help`.

Exit status: `0` success, `1` failure (also a `sync` with any failed source),
`2` usage error, `69` command unavailable in this build, `75` another
[committer](architecture.md#terms) holds the namespace lease, `78` invalid configuration. Usage
errors name the argument and the usage line; they never echo what was typed.

For storage failures, opt into private JSONL operation diagnostics with
`FYRR_OBJECT_STORE_DIAGNOSTICS=/absolute/private/objects.jsonl`; see
[object-store diagnostics](operations.md#object-store-diagnostics) for fields,
privacy rules and unavailable transport facts.

Terms such as serving head, publication and envelope are defined in
[How fyrr works](architecture.md#terms).

## `fyrr config`

| Command | Effect |
| --- | --- |
| `config check [--no-env]` | Validates the file, its cross-field rules and, in the full build, every source against the installed [provider](architecture.md#terms)'s schema; then checks that every referenced environment variable is set unless `--no-env`. Prints warnings and errors with key paths; exit 78 on invalid configuration, or 1 when installed providers cannot be discovered. |
| `config show` | Prints every effective value with its origin (`default`, `file`, `env`), sorted by key. Secret fields are redacted; in the reader-only build, or when a provider is unknown or refused, a source's whole `settings` and `scope` are redacted. |
| `config schema` | Prints the configuration JSON Schema; the full build embeds each installed provider's `settings` and `scope` schema and applies it to `sources[]` by `type`. |

## `fyrr reader`

| Command | Effect |
| --- | --- |
| `reader check` | Runs the startup path without modifying S3 or calling providers (it may create and populate the local mirror): reads the serving head, policy and identity, admits the pinned publication. Prints readiness JSON; exits nonzero when not ready. |
| `reader prewarm` | Verifies and prewarms the pinned immutable files into the mirror, without provider calls; prints readiness. |
| `reader search QUERY [--limit N] [--output metadata\|excerpt\|content]` | Runs one search as the configured stdio subject and prints the same JSON as the MCP `search` tool, with `--limit` (default 50) documents and `--output` (default `excerpt`) text per hit. A query is required; query-less listing exists only through the MCP tool. A query may start with `-`; put `--` before one that is exactly an option such as `--limit`. |
| `reader serve` | Serves MCP: over stdio for `identity.stdio_user`, or over Streamable HTTP at `/mcp` when `[reader.http]` is configured. |

## `fyrr writer`

The writer runs every provider in isolated `__worker` child processes. The
`__worker` subcommand is internal.

| Command | Effect |
| --- | --- |
| `writer providers` | Lists installed connector providers with distribution, version, SDK requirement and status (`ok` or a refusal reason); exits 1 if any is refused. |
| `writer namespace create` | Creates the namespace's policy, identity and serving head, and records its embedding, index and retrieval settings from the configuration file. Idempotent for the same settings; other settings fail with `namespace_settings_fixed`. |
| `writer namespace read-only ENABLED` | `ENABLED` is `true` or `false`. Freezes the namespace or thaws it, as a new policy generation under the namespace lease, and prints `{"read_only":…,"changed":…}`; a repeated call changes nothing. While it is frozen API writes and connector syncs fail with `namespace_read_only`, and envelopes prepared before the switch are still committed; maintenance and the switch itself still work. See [Freezing a namespace](http-api.md#freezing-a-namespace). |
| `writer namespace copy --to-config FILE` | Copies the configured namespace, as the one version its head serves when the command starts, into the `[storage]` of the configuration file `FILE` (another bucket or region), under the same name. The source is only read, so it can be written meanwhile. The copy has the source's policy, identity, document metadata, publication sequence and cursor sequences, so readers find the same documents with the same access and the next write of a source is newer than every row the copy holds. The keyed hashes in the rows are made with the name, so a copy cannot take another name, and `FILE`'s `identity.hashing_key` must be the source's (`hashing_key_mismatch` otherwise, before anything is written). A copy in the same storage, which would need another name, fails with `copy_same_storage`. The copy is a namespace of its own and writable, also when the source is frozen. It has no publication history, receipts, envelopes or stored credentials, and its connector cursors restart from the beginning of their source. A namespace whose identity holds snapshots (`permissions = "sync"` sources or a directory) fails with `copy_inputs_unsupported`. The head is written last: a copy that stopped leaves no namespace, and running the command again removes what it left and starts over; once the copy exists the command fails with `namespace_exists`. Run at most one copy to a destination at a time. Prints `{"copied":true,"namespace":…,"publication_sequence":…,"lance_version":…,"files":…,"bytes":…,"seconds":…}`. See [Backup and copy](operations.md#backup-and-copy). |
| `writer namespace delete [--export DIR] [--delete-data NAME]` | Without either flag prints the deletion plan and deletes nothing. `--export DIR` checks the directory, a byte-for-byte export of the namespace's objects, against storage first; without it `--delete-data NAME` must repeat the namespace name (`delete_confirmation_invalid` otherwise). The command removes the serving head, then every object under the namespace's prefix, the deletion fence last, and prints `{"deleted":true}`. Run at most one `writer namespace create` or `delete` for a namespace at a time; concurrent administration commands on the same namespace are not supported. Stop every writer of the namespace first: the command waits until the fence is `writer.commit.lease_ttl` old (default 1 minute) and, after the last pass, once more, and lists again. An object that appeared meanwhile is removed with `--delete-data` and fails the command with `export_invalid` with `--export`, which removes only the objects it verified. Only current objects are removed: on a versioned S3 bucket, noncurrent versions remain until a lifecycle rule expires them. An interrupted deletion finishes when the command is run again with the same confirmation (`--export` is then checked against the objects that remain); until then no source takes a cursor, and `writer namespace create` fails with `namespace_deleting` while any object remains under the namespace's prefix. Objects left by an interrupted `writer namespace create` are removed by the same command. Afterwards the name can be created again as a new namespace. The same deletion is `DELETE /v1/namespaces/{namespace}` on the [writer listener](identity.md#identityapi_keys). |
| `writer namespace list [--prefix P]` | Prints `{"namespaces":[...]}`: the namespaces of the storage root, in name order, that start with `P` (lowercase letters, digits and `-`, 1 to 63 characters). One delimited listing of `namespaces/`, no other request. A name is listed while any object exists under it, so a namespace whose deletion has not finished is listed. More than 10,000 matching names fail with `too_many_namespaces`. The same list is `GET /v1/namespaces` on the [reader](http-api.md#listing-namespaces). |
| `writer sync [SOURCES…] [--kind KIND]… [--full] [--dry-run] [--due] [--jobs N] [--timeout DURATION]` | Runs one worker per source (default: all configured sources), at most `--jobs` (default 4, 1–256) at once, each killed after `--timeout` (default `10m`), with a diagnostic naming the timeout and flag. For large initial loads use e.g. `--full --timeout 1h`. `--kind` selects `content`, `permissions`, `members` or `reconcile` (repeatable; default: every kind the provider implements and the connection type uses). `--full` prepares whole sources (bulk load). `--dry-run` runs the providers and validates their output without writing. `--due` runs only kinds whose configured interval has passed. The CLI rejects `--dry-run` combined with `--due`. |
| `writer sync --directory [--timeout DURATION]` | Pulls one complete directory snapshot from the configured directory provider. The pull holds the namespace lease from before the read until its publication. Source IDs and connector-only options such as `--full` and `--jobs` cause a usage error. |
| `writer commit [--stage-timings]` | Acquires the namespace lease and publishes pending envelopes in bounded groups. Prints the latest publication (ID, [Lance](architecture.md#terms) version, envelope IDs); a recovery-only publication has no envelope IDs. `--stage-timings` writes bounded stage timings to stderr. A competing committer exits 75 at once. |
| `writer commit --receipt ENVELOPE_ID` | Looks up the receipt of an earlier publication by envelope ID; writes nothing. |
| `writer serve [--jobs N] [--timeout DURATION]` | Foreground scheduler: runs due source workers and directory pulls, at most `--jobs` (default 4, 1–256) at once, each killed after `--timeout` (default `10m`), and commits after each coalescing timer (`writer.commit.coalescing_timer`, default 60 s). `writer.serve.committer = false` keeps syncing without publishing. After a tick it runs `writer maintain` when the served manifest shows a vector index with more than 64 segments and two neighbors that fit one merge step, or, at most once per `writer.version_grace`, a fragment with deletions or no maintenance for `writer.purge_window` minus `writer.version_grace` (or `writer.max_envelope_age` if shorter); `writer.serve.maintenance = false` turns that off. `SIGINT`/`SIGTERM` stops the workers, finishes the current commit group or maintenance run and releases the lease. |
| `writer status` | Prints envelope and pending-envelope counts, quarantined entries per reason, the data files (`fragments`) and the segments of each index (`index_segments`) of the served version, the directory's identity generation, the age of the last permission and member read per source (`source_reads`) and a value-free `warnings` list, relative to the printed `content_sequence`. Counts are exact in a quiescent namespace. |
| `writer maintain` | Runs lease-held maintenance in bounded steps: purges unreadable rows, compacts, appends or merges index deltas and cleans retired versions and old envelopes. |
| `writer verify` | Scans the pinned publication completely: logical keys, document state, revisions, finite nonzero vectors, index coverage. Reports `incompatible_index` when index kinds, coverage or the publication's recorded analyzer and retrieval policy do not match the namespace's settings. A missing readers index remains `readers_index_missing`. Needs only namespace storage. |
| `writer restore PUBLICATION` | Publishes a new fenced version from a retained publication, with the current policy, identity, permissions and tombstones. |
| `writer rollback` | Restores the preceding retained publication. |
| `writer source retire SOURCE` | Denies the source to every reader from its next version check on and rejects its further envelopes. Final; kept by restore. |
| `writer source purge SOURCE [--confirm]` | Without `--confirm` prints the plan (rows, envelopes, cursors). With it retires the source and deletes its rows, state, envelopes and cursors; each step is idempotent, so an interrupted purge is run again. |
| `writer keys` | Publishes the configured [API keys](identity.md#identityapi_keys) of the deployment as one artifact under the storage root, written by compare-and-swap: each key's keyed hash with its roles and namespace prefix, never a secret, name or reader token. The artifact replaces the published set and is not written again when current. If a secret is unset or malformed, or two keys share a secret, the command exits 1 (`configuration_invalid`) and leaves the published set unchanged. A competing publisher exits 75 (`committer_busy`). Needs no namespace and no lease. |
| `writer directory --snapshot FILE` | Publishes a complete hashed directory snapshot under the namespace lease. |
| `writer credential --name NAME --from-env VARIABLE` | Stores the variable's value as an encrypted credential record under the namespace lease; `[writer.credentials]` maps a provider's environment reference to it. |

### Finding providers

See [provider installation and discovery](deployment.md#providers-in-the-image).

### Namespace deletion failures

| Code | Meaning |
| --- | --- |
| `conditional_delete_unsupported` | The S3 server did not prove that it honors conditional deletion. The serving head is retained; use a backend supporting `If-Match` on DELETE. |
| `delete_confirmation_invalid` | `--delete-data` does not repeat the namespace name; nothing was deleted. |
| `namespace_deleting` | Objects remain under the namespace's prefix, as after a deletion that removed the serving head and did not finish. Run `writer namespace delete` again before creating the namespace. |

### Namespace copy failures

| Code | Meaning |
| --- | --- |
| `copy_incomplete` | A file of the version is missing in the copy, as when the source removed it while Lance copied. Nothing was published; run the command again. |
| `copy_inputs_unsupported` | The source's identity holds snapshots of a `permissions = "sync"` source or a directory. They are sealed for the namespace's storage, so a copy could not read them; sync the permissions into a new namespace instead. |
| `copy_same_storage` | `--to-config` names the source's storage. A copy under another name would need new hashes and is not supported. |
| `copy_source_changed` | The source was deleted and created again while it was copied. Nothing was published. |
| `hashing_key_mismatch` | The `identity.hashing_key` of `--to-config` is not the source's. Nothing was written. |
| `namespace_deleting` | Objects that are not a copy's own remain under the namespace's prefix in the destination, as after a deletion that did not finish. Run `writer namespace delete` for it first. |
| `namespace_exists` | The destination already holds the namespace. |
| `namespace_not_published` | The source has no content yet. |
