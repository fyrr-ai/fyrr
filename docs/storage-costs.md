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

## Reader

A reader serves searches from its mirror, a local copy of the files of the
publication it serves (`reader.mirror_path`). Results change only with a new
publication, so a running reader asks storage nothing else.

| State | Requests |
| --- | --- |
| Searching, mirror warm | none (0 measured) |
| Idle or busy, nothing published | one `GET` of the serving head per `reader.version_check_interval` (30 s by default) |
| Start on an empty mirror | 27 for a one-document namespace: the control artifacts, then each index and data file once |
| Switch to a new publication | 25 for a one-document change: the new manifest and each file the publication adds or rewrites, plus three more reads of the head |
| Restart on a warm mirror | the control artifacts; files in the mirror are verified locally (from the code, not measured) |
| HTTP readiness probe | one `GET` of the serving head per `/readyz` request |

A data file of up to 2 MiB is downloaded whole on its first read, so a file
is read once, not once in pages and again when the mirror warms. Open and
switch counts grow with the files of a publication.

The version check is what a reader costs while it runs: 86,400 `GET` a month
at the default interval, about $0.035 per reader process, however many
searches it answers. A longer interval does not cost less, because the
reader checks at least every 30 s:

| `version_check_interval` | `GET` per month | Cost per month |
| --- | --- | --- |
| `1s` | 2,592,000 | $1.04 |
| `10s` | 259,200 | $0.10 |
| `30s` (default) | 86,400 | $0.035 |

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
| `PUT` | 0 |

That is 13 `GET` or `HEAD` and 1 `LIST` per tick. It does not grow with the
number of publications; the `LIST` grows by one per 1,000 envelopes kept
(`writer maintenance` removes applied ones). Both parts scale with the
timer:

| `coalescing_timer` | Ticks per month | Cost per month |
| --- | --- | --- |
| `10s` | 259,200 | $2.64 |
| `60s` (default) | 43,200 | $0.44 |
| `5m` | 8,640 | $0.09 |

At the default timer the 561,600 `GET` or `HEAD` cost $0.22 and the 43,200
`LIST` $0.22. With `writer.serve.committer = false` only the watch task
remains: one head `GET` per timer, 43,200 a month, $0.017.

**The head watch** is a task that reads the head once per timer (the second
head read above). A changed head, which carries rejections and rewinds,
wakes every source.

**Sync workers** are started per source, and each source sleeps until a
kind of it is due (`sources.intervals`: content 15 min and reconcile 1 day
by default, plus permissions and members for a source with
`permissions = "sync"`). A worker also runs at the start of the service and
whenever the head changes. It reads the head and the cursor of each kind
first; a kind that is not due costs no lease and no listing.

| Per worker, estimated from the code | Requests |
| --- | --- |
| Each kind not due | 2 `GET`: head, cursor |
| Each kind due, nothing changed | 2 `PUT` (cursor lease, cursor), 1 `LIST`, about 9 `GET` |
| History, once per worker | 1 `GET` per publication of the namespace |
| Each kind with changes | the above, plus the envelopes it writes |

A due kind costs about $0.0000186 per pass. A due pass writes cursor
objects, so the "no writes" rule holds while no kind is due:

| Kind interval | Passes per month | Cost per month |
| --- | --- | --- |
| `15m` (default content) | 2,880 | $0.05 |
| `5m` | 8,640 | $0.16 |
| `1m` | 43,200 | $0.80 |

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
| Each reader | the new head, manifest and policy, then each file the publication adds or rewrites, once (25 measured for a one-document change) |

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

1. **Readers:** `R` × $0.035 for the version check at 30 s.
2. **Writer service:** $0.44 for the idle committer at 60 s.
3. **New versions:** the table above, for the writer and for each reader.
4. **Sync workers:** the due passes of each kind of each source, plus the
   history walk: $0.0004 per 1,000 publications per worker.

Examples for the first month of a new namespace, whose history grows from
nothing over the month (on average half the month's publications); all
sources have `permissions = "fixed"`, so two kinds each, content every
15 min and reconcile daily. The sync figures are estimates:

| Deployment | Readers | Writer idle | New versions | Sync workers | First month |
| --- | --- | --- | --- | --- | --- |
| Quiet: 1 reader, 1 source, one change a day | $0.04 | $0.44 | $0.02 | about $0.07 | about $0.57 |
| Team: 2 readers, 3 sources, changes every 15 min | $0.07 | $0.44 | $1.68 | about $10 | about $12 |

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
