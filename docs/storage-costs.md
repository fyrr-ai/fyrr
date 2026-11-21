# Storage request costs

> - An object store bills every request. This page counts the requests fyrr
>   makes in each state, so a deployment can be priced before it runs.
> - Prices below are Amazon S3 Standard list prices; other stores and
>   regions differ. Transfer between S3 and compute in the same region is
>   not billed. Storage (GB-months) and transfer out of the region are not
>   covered here.
> - Counts are per namespace and per process, measured on the local store
>   of the CI tests unless marked as an estimate.

| Request | Price per 1,000 |
| --- | --- |
| `GET`, `HEAD` | $0.0004 |
| `PUT`, `COPY`, `POST`, `LIST` | $0.005 |

## Reader

A reader serves searches from its mirror, a local copy of the files of the
publication it serves (`reader.mirror_path`). Searches of a warm reader make
no storage request.

| State | Requests |
| --- | --- |
| Searching, mirror warm | none |
| Idle or busy, nothing published | one `GET` of the serving head per `reader.version_check_interval` (30 s by default) |
| Start on an empty mirror | the publication's control artifacts, then each index and data file once while it opens and warms |
| Switch to a new publication | the new manifest and three more reads of the head, then each file the new publication adds or rewrites |
| Restart on a warm mirror | the control artifacts; files already in the mirror are verified locally (from the code, not measured) |
| HTTP readiness probe | one `GET` of the serving head per `/readyz` request |

The version check is what a reader costs while it runs: 86,400 `GET` a month
at the default interval, about $0.035 per reader process, however many
searches it answers. A shorter interval costs proportionally more; a longer
one costs no less, because the reader checks at least every 30 s:

| `version_check_interval` | `GET` per month | Cost per month |
| --- | --- | --- |
| `1s` | 2,592,000 | $1.04 |
| `10s` | 259,200 | $0.10 |
| `30s` (default) | 86,400 | $0.035 |

A mirror smaller than the publication (`reader.mirror_max_bytes`) cannot keep
every file, and searches that reach a file it does not hold read it from
storage. Size the mirror to the namespace when searches are frequent.

## Writer

`fyrr writer serve` runs two things that read storage while nothing changes.

**The committer** wakes every `writer.commit.coalescing_timer` (60 s by
default). An idle tick takes and releases the namespace lease, reads the head,
the policy and the served manifest, lists the pending envelopes, opens the
dataset and publishes the head: about 3 `PUT`, 1 `LIST` and 15 `GET` or
`HEAD`. That does not grow with the number of publications; the `LIST` grows
by one per 1,000 envelopes kept (`writer maintenance` removes applied ones).

| | Per month at the default timer | Cost per month |
| --- | --- | --- |
| `PUT` | 129,600 | $0.65 |
| `LIST` | 43,200 | $0.22 |
| `GET`, `HEAD` | 648,000 | $0.26 |
| Total | | $1.12 |

**Sync checks** start a worker for each source at most a minute after its
last one finished (sooner when an interval of the source is shorter), and
the worker syncs each kind of the source that is due (`sources.intervals`):
content and reconcile for a source with `permissions = "fixed"`, and also
permissions and members for one with `permissions = "sync"`. A kind that is
not due still costs, per check:

- 2 `PUT` and 1 `LIST`: the cursor lease and the envelope listing;
- about 9 `GET` or `HEAD`: head, policy, cursor, the dataset version;
- one `GET` per publication in the history and one per applied envelope
  still kept, because each worker is a new process that reads the history
  again.

| Part, per kind of a source | Per month | Cost per month |
| --- | --- | --- |
| Fixed: 2 `PUT`, 1 `LIST`, 9 `GET` per minute | 2.0 M | $0.80 |
| History: per 1,000 publications and kept envelopes | 43.2 M `GET` | $17.28 |

The history part is the one that grows: a source that publishes every
15 minutes adds 2,880 publications a month. Until sync checks stop
re-reading it, keep the number of sources and kinds of a long-running
namespace in mind.

A sync or a commit that has work writes what it publishes: envelopes, Lance
data and index files, a manifest and the head. Those requests grow with the
changed documents, not with time.

## New versions

Every publication costs requests twice: once when the writer writes it, and
once in every reader that switches to it.

| Side | Requests per publication |
| --- | --- |
| Writer | the envelopes the syncs wrote, the Lance data and index files of the change, the manifest and the head: mostly `PUT`, growing with the changed documents |
| Each reader | the new head, manifest and policy, then each file the publication adds or rewrites, once |

In the test namespace a one-document change made a reader switch with 25 to
27 storage requests, warming of the new generation included. Changes that
rewrite more index files cost more. The estimates below assume 100 requests
per publication on each side; that is an assumption, not a bound.

| Publications | Per month | Writer (assumed) | Each reader (assumed) |
| --- | --- | --- | --- |
| one a day | 30 | $0.02 | under $0.01 |
| every 15 min (the default content interval) | 2,880 | $1.44 | $0.12 |
| every minute | 43,200 | $21.60 | $1.73 |

The writer figures price all 100 requests as `PUT`, the reader figures as
`GET`. Nothing is published when nothing changed: a sync that finds no
changes writes no envelope, and an idle commit publishes only the head.

## Estimating a deployment

A month of a deployment adds up from four parts:

1. **Readers:** `R` × $0.035 for the version check at 30 s.
2. **Writer service:** $1.12 for the idle committer at 60 s.
3. **New versions:** the table above, for the writer and for each reader.
4. **Sync checks:** $0.80 per kind of each source, plus $0.0173 per kind
   and month for every publication and kept envelope of the namespace.

Examples for the first month of a new namespace, whose history grows from
nothing over the month (on average half the month's publications), with
envelopes removed by maintenance:

| Deployment | Readers | Writer idle | New versions | Sync checks | First month |
| --- | --- | --- | --- | --- | --- |
| Quiet: 1 reader, 1 source with 2 kinds, one change a day | $0.04 | $1.12 | $0.02 | $2.13 | about $3.30 |
| Team: 2 readers, 3 sources with 2 kinds, changes every 15 min | $0.07 | $1.12 | $1.68 | about $154 | about $157 |

In the team example almost all of the cost is sync checks re-reading the
history. At the end of the first month it runs at about $300 a month, and
it rises by about $300 a month for every further month of publishing at
this rate.

Other costs to add when they apply:

- **Each process pays its own:** a second `writer serve` on the same
  namespace pays its idle costs again (`writer.serve.committer = false`
  turns its committer off); `writer sync` and `writer commit` run from a
  scheduler pay a check's fixed requests at every run.
- **Directory pulls:** Keycloak and snapshot directories are pulled every
  `identity.directory.sync_interval` (15 min), and a SCIM directory is
  attested at every committer tick; neither is counted above.
- **Maintenance:** `writer maintenance` lists and reads the envelopes and
  dataset files it cleans up, and keeps the envelope listings short.
- **Lambda and other environments without a disk that outlives them:** each
  execution environment fills its own mirror, so every cold start reads the
  publication's files once; a frozen environment makes no version check.
- **Bucket versioning:** the head is rewritten about three times a minute;
  add a lifecycle rule for noncurrent versions.

The request counts of a running reader and an idle writer service are
checked in CI (`crates/fyrr/tests/dense_path/storage_budget.rs`): a change
that adds requests fails there before it reaches a deployment.
