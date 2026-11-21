# Storage requests and their cost

> **In short**
>
> - A warm reader answers searches from its mirror and makes no storage
>   request per search. Its only steady request is the version check: one
>   read of `head.json` per `reader.version_check_interval` (30 s).
> - A reader reads each file of a publication once, when it opens it or
>   switches to a newer one, and keeps it in its mirror.
> - An idle `writer serve` makes about 19 requests per committer tick
>   (`writer.commit.coalescing_timer`, 1 min), three of them writes. It also
>   starts a sync worker per source about once a minute, which costs about as much
>   again per source and grows with the retained envelopes
>   ([#80](https://github.com/fyrr-ai/fyrr/issues/80)).
> - A test holds the reader phases and the committer tick to these numbers,
>   so a change that adds requests there fails CI.

Object stores bill every request. On Amazon S3 Standard a GET or HEAD costs
$0.0004 per 1,000 and a PUT, COPY or LIST $0.005 per 1,000 (prices at the
time of writing; check your region). Stored bytes are billed per GB-month.
Transfer to compute in the same region is free, so beyond the stored bytes
the request count decides the bill; a reader outside the bucket's region also pays
transfer for every file it mirrors. The figures below come from the storage
budget test (`crates/fyrr/tests/dense_path/storage_budget.rs`), which counts
every request a running process makes, on local storage; on S3 a large
upload takes several requests (multipart).

## Reader

| Phase | Requests |
| --- | --- |
| A search on a warm reader | 0 |
| An idle reader | 1 GET of `head.json` per `version_check_interval` (30 s by default and at most) |
| An HTTP reader's `/readyz` probe | 1 GET of `head.json` per probe |
| Opening on an empty mirror and warming | Each control artifact and index file once; data files once each, or in the 4 KiB pages a search needs until warming copies them (28 requests for a one-document namespace) |
| Switching to a new publication | Its new files once (26 requests in the same test), plus three head reads |
| A request that wakes a reader which did not run for an interval or more (a frozen Lambda environment, a suspended host) | 1 GET of `head.json` |

The version check is the cost that does not depend on traffic: at the
default 30 s it is 86,400 GETs a month, about $0.03 per reader. The reader
caps the interval at 30 s, so a longer `version_check_interval` does not
lower it. A frozen Lambda environment makes no idle checks;
the first request after an interval pays one. A readiness probe every 10 s
adds 259,200 GETs, about $0.10, a month per reader. A thousand searches a
second cost nothing more, because searches check their grants against the
head that check read.

The first searches after open pay for the files they touch until the
generation is warm; [AWS Lambda](lambda.md) environments repeat this at
every cold start, each with its own mirror.

## Writer

An idle committer tick takes and releases the namespace lease, reads the
head, the policy, the identity and the served publication manifest, lists
`envelopes/`, opens the dataset and publishes the head: about 19 requests,
3 of them PUTs and 1 a LIST. At the default 1 min timer that is about
43,000 ticks and 820,000 requests a month, about $1.10. The tick does not
grow with the number of publications; its listing takes one request per
1,000 retained envelopes. `writer.serve.committer = false` removes it on
hosts that only sync. A long commit renews its lease with further
`head.json` PUTs.

A commit adds the envelopes, Lance data and index files and one
publication manifest. A sync writes one envelope per page of changes and
updates its cursor.

`writer serve` also starts a sync worker for every source about a minute
after its previous run ends (sooner when a source interval is shorter), and
the worker decides only after several reads and its cursor
lease writes that nothing is due: about 20 requests per source per minute on
a namespace with a handful of retained envelopes, about $1 per source a
month. The worker reads the retained envelopes each time, so this grows with
them until `writer maintain` cleans them up. The budget test does not cover
it; [#80](https://github.com/fyrr-ai/fyrr/issues/80) tracks the fix.

These figures are for an idle service without a SCIM directory, measured
on a small namespace. With SCIM each tick also attests the directory; with a
Keycloak or snapshot provider `writer serve` pulls the directory every
`identity.directory.sync_interval`. The explicit commands `writer
maintain`, `writer verify` (which reads the whole publication), restores and
source purges make their own requests, which this page does not count.

## Estimating a deployment

Requests per month, with *r* readers, *s* sources and the defaults:

- readers: *r* × 86,400 GETs (more with a shorter interval or readiness
  probes; a frozen Lambda environment fewer);
- committer: about 820,000 requests, of which 130,000 PUTs and 43,000 LISTs;
- sync workers: about *s* × 900,000 requests or more until #80 is fixed;
- each publication: its new files once per reader.

Searches add nothing once a reader is warm.

## Keeping it that way

`a_running_reader_stays_within_its_storage_request_budget` and
`an_idle_writer_service_stays_within_its_storage_request_budget` run in CI
and fail when a phase makes more requests than its budget, printing every
request it made. Raise a budget only on purpose.
