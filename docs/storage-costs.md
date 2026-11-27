# Storage request costs

> - An object store bills every request. This page counts the requests fyrr
>   makes in each state, so a deployment can be priced before it runs.
> - Prices are Amazon S3 Standard list prices; other stores and regions
>   differ. Transfer between S3 and compute in the same region is not billed.
>   Storage (GB-months) and transfer out of the region are not covered.
> - Counts are per namespace and per process. They are measured on the local
>   store of the CI tests unless marked as an estimate. A month is 30 days.

| Request | Price per 1,000 |
| --- | --- |
| `GET`, `HEAD` | $0.0004 |
| `PUT`, `COPY`, `POST`, `LIST` | $0.005 |
| `DELETE`, multipart upload cancel | free |

## Reader

A reader serves searches from its mirror, a local copy of the files of the
publication it serves (`reader.mirror_path`). Results change only with a new
publication, so a running reader asks storage nothing else.

| State | Requests |
| --- | --- |
| Searching, mirror warm | none (0 measured) |
| Idle or busy, nothing published | one `GET` of the serving head per `reader.version_check_interval` (30 s by default) |
| Start on an empty mirror | at most 27 for a one-document namespace (CI bound, head reads not counted): the control artifacts, then each index and data file once |
| Switch to a new publication | at most 25 for a one-document change (CI bound): the new manifest and each file the publication adds or rewrites, plus three more reads of the head |
| First dense query of a subject on a vector index merged by `writer maintain`, empty mirror | the two index files, one `GET` each, however many partitions the index has, when the mirror has free room for them (otherwise pages, as before); the query adds the data-file pages it reads (94 in all for a 6,000-row namespace, CI bound) |
| Restart on a warm mirror | the control artifacts; files in the mirror are verified locally (from the code, not measured) |
| HTTP reader that declares API keys | one more `GET`, of `api-keys.json` in the storage root, at start and per `reader.version_check_interval`; none per request while it is readable (CI bound) |
| HTTP JSON request with `consistency: "strong"` | one `GET` of the serving head when it has not moved (CI bound); the requests of a switch when it has |
| HTTP readiness probe | one `GET` of the serving head per `/readyz` request, plus one `GET` of the identity artifact on the first request and when the identity changes |

A data file of up to 2 MiB is downloaded whole on its first read when it
fits the mirror, so it is read once. A larger file, or one that does not fit,
is read in pages first and copied whole while the mirror warms. Open and
switch counts grow with the files of a publication.

The version check is what a reader costs while it runs: 86,400 `GET` a month
at the default interval, about $0.03 per reader process, however many
searches it answers. A longer interval does not cost less, because the
reader checks at least every 30 s:

| `version_check_interval` | `GET` per month | Cost per month |
| --- | --- | --- |
| `1s` | 2,592,000 | $1.04 |
| `10s` | 259,200 | $0.10 |
| `30s` (default) | 86,400 | $0.03 |

A mirror smaller than the publication (`reader.mirror_max_bytes`) cannot keep
every file, and searches that reach a file it does not hold read it from
storage. Size the mirror to the namespace when searches are frequent.

## Writer service

`fyrr writer serve` runs three things that read storage while nothing
changes. **An idle writer makes no writes:** no lease, no head, no cursor.
The CI test checks this over a window with the service running.

**The committer** wakes every `writer.commit.coalescing_timer` (60 s by
default). An idle tick reads and lists; it takes no lease and publishes
nothing:

| Request, per idle tick | Count |
| --- | --- |
| `GET` head | 2 (the committer's and the watch task's) |
| `GET` policy | 2 |
| `GET` identity | 1 |
| `GET` publication manifest | 1 |
| `HEAD` and `GET` of the dataset | 7 |
| `LIST` envelopes | 1 |
| `LIST` of the dataset's `_versions/` on S3 | 1 (estimate: the local store of the tests does not show it) |
| `PUT` | 0 |

That is 13 `GET` or `HEAD` and at least 2 `LIST` per tick on S3 Standard
(the measured run saw 13 and 1). It does not grow with the number of
publications; the envelope `LIST` grows by one per 1,000 envelopes kept
(`writer maintenance` removes applied ones). Both parts scale with the
timer:

| `coalescing_timer` | Ticks per month | Cost per month |
| --- | --- | --- |
| `10s` | 259,200 | $3.94 |
| `60s` (default) | 43,200 | $0.66 |
| `5m` | 8,640 | $0.13 |

At the default timer the 561,600 `GET` or `HEAD` cost $0.22 and the 86,400
`LIST` at least $0.43. With `writer.serve.committer = false` only the watch
task remains: one head `GET` per timer, 43,200 a month, $0.017.

**The head watch** is a task that reads the head once per timer (the second
head read above). A changed head, which carries rejections, wakes every
source.

**Sync workers** are started per source, and each source sleeps until a
kind of it is due (`sources.intervals`: content 15 min and reconcile 1 day
by default; a source with `permissions = "sync"` also runs the permissions
and members kinds its provider supports, members only when
`intervals.members` is set). A worker also runs at the start of the service
and whenever the head changes. A kind that is not due costs no lease and no
envelope listing.

| Per worker, estimated from the code | Requests |
| --- | --- |
| Each kind not due | 4 `GET` outside the history: head and policy (retirement check), head and cursor (scheduler) |
| Each kind due, nothing changed | at least 3 `PUT` (lease take, cursor write, lease release), 1 `LIST`, about 9 `GET` |
| History, once per worker | 1 `GET` per publication of the namespace, plus the rejection records they carry |
| First due pass of a worker | also 1 `GET` per retained envelope, applied ones included |
| Each kind with changes | the above, plus the envelopes it writes |

After an earlier rejection, a permissions or members kind may take the lease
and list the envelopes before it decides it is not due. The envelope reads
depend on retention: `writer maintenance` removes applied envelopes, and
kept ones add a `GET` each to every worker.

A due kind costs about $0.0000236 per pass. A due pass writes cursor
objects, so the "no writes" rule holds while no kind is due:

| Kind interval | Passes per month | Cost per month |
| --- | --- | --- |
| `15m` (default content) | 2,880 | $0.07 |
| `5m` | 8,640 | $0.20 |
| `1m` | 43,200 | $1.02 |

The history part grows with the namespace: a worker reads each publication
once, and a new process reads them all again. A worker costs $0.0004 per
1,000 publications; a source with a change every 15 minutes adds 2,880
publications a month and runs up to two workers per publication (its own
due pass and the wake for the head change it caused).

A sync or a commit that has work writes what it publishes: envelopes, Lance
data and index files, a manifest and the head. Those requests grow with the
changed documents, not with time.

## New versions

Every publication costs requests twice: once when the writer writes it, and
once in every reader that switches to it.

| Side | Requests per publication |
| --- | --- |
| Writer | the envelopes the syncs wrote, the Lance data and index files of the change, the manifest and the head: mostly `PUT`, growing with the changed documents |
| Each reader | the new head, manifest and policy, then each file the publication adds or rewrites, once (at most 25 for a one-document change, the CI bound) |

Changes that rewrite more index files cost more. The estimates below assume
100 requests per publication on each side; that is an assumption, not a
bound.

| Publications | Per month | Writer (assumed) | Each reader (assumed) |
| --- | --- | --- | --- |
| one a day | 30 | $0.02 | under $0.01 |
| every 15 min | 2,880 | $1.44 | $0.12 |
| every minute | 43,200 | $21.60 | $1.73 |

The writer figures price all 100 requests as `PUT`, the reader figures as
`GET`. Nothing is published when nothing changed: a sync that finds no
changes writes no envelope, and an idle commit publishes nothing.

## Estimating a deployment

A month of a deployment adds up from four parts:

1. **Readers:** `R` × $0.03 for the version check at 30 s.
2. **Writer service:** about $0.66 for the idle committer at 60 s.
3. **New versions:** the table above, for the writer and for each reader.
4. **Sync workers:** the due passes of each kind of each source, plus the
   history walk: $0.0004 per 1,000 publications per worker. The examples
   assume that maintenance removes applied envelopes.

Examples for the first month of a new namespace, whose history grows from
nothing over the month (on average half the month's publications); all
sources have `permissions = "fixed"`, so two kinds each, content every
15 min and reconcile daily. The sync figures are estimates:

| Deployment | Readers | Writer idle | New versions | Sync workers | First month |
| --- | --- | --- | --- | --- | --- |
| Quiet: 1 reader, 1 source, one change a day | $0.03 | $0.66 | $0.02 | about $0.10 | about $0.81 |
| Team: 2 readers, 3 sources, changes every 15 min | $0.07 | $0.66 | $1.67 | about $10 | about $13 |

In the team example most of the cost is the history walk of the sync
workers. At the end of the first month it runs at about $20 a month, and it
rises by about $20 a month for every further month of publishing at this
rate.

Other costs to add when they apply:

- **Each process pays its own:** a second `writer serve` on the same
  namespace pays its idle costs again (`writer.serve.committer = false`
  turns its committer off); `writer sync` and `writer commit` run from a
  scheduler pay a worker's requests at every run.
- **Directory pulls:** Keycloak and snapshot directories are pulled every
  `identity.directory.sync_interval` (15 min). A pull holds the namespace
  lease, so it writes at least the lease twice (about 5,800 `PUT` a month,
  $0.03, an estimate), plus the provider's own requests.
- **Maintenance:** `writer maintenance` lists and reads the envelopes and
  dataset files it cleans up, and keeps the envelope listings short.
- **Lambda and other environments without a disk that outlives them:** each
  execution environment fills its own mirror, so every cold start reads the
  publication's files once; a frozen environment makes no version check.

The request counts of a running reader and an idle writer service are
checked in CI (`crates/fyrr/tests/dense_path/storage_budget.rs`): a change
that adds requests, or an idle write, fails there before it reaches a
deployment.
