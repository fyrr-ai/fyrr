# Query latency and recall at 3,000,000 rows

`scripts/bench-scale` drives the real `fyrr writer serve`, `fyrr writer
maintain` and `fyrr reader serve` executables on one local storage. Python
generates the rows, sends the requests and scores the answers; every latency,
request count, byte count and memory figure comes from those executables or
from `/proc` and the reader's `/metrics`. This answers
[#110](https://github.com/fyrr-ai/fyrr/issues/110) for one corpus shape and
feeds [#106](https://github.com/fyrr-ai/fyrr/issues/106),
[#194](https://github.com/fyrr-ai/fyrr/issues/194) and the cold-read issues
[#103](https://github.com/fyrr-ai/fyrr/issues/103) and
[#104](https://github.com/fyrr-ai/fyrr/issues/104).

## What was measured

- **Corpus.** 3,000,000 rows of 128 dimensions (`f16`, `cosine`, the defaults
  `dense_partition_rows = 550`, `dense_rq_bits = 1`), synthetic: 1,000 Gaussian
  clusters (noise 1.0 around centres of unit spread), and a text of 24 words
  per row drawn from a Zipf vocabulary of 100,000 words. Rows are written
  through `POST /v1/namespaces/{ns}/documents` in 60 requests of 50,000 rows,
  one after the other. No embedding call is in the path.
- **Subjects.** The namespace is not open. Every row is readable by `grp:all`,
  every 10th row also by `usr:p10`, every 100th also by `usr:p1`. Three API
  keys read as `grp:all` (**100%** of the rows), `usr:p10` (**10%**,
  300,000 rows) and `usr:p1` (**1%**, 30,000 rows). The restricted rows are an
  even sample of every cluster.
- **Queries.** Dense: a `vector` alone. Full-text: a `query` of 3 words (Zipf
  rank 10 or more), no embedding. Hybrid: both. `limit` 10, `output:
  metadata`, one client, one request at a time. 100 queries per lane and
  subject after the first. Query vectors are drawn from the corpus distribution
  and are not rows of it.
- **Recall.** Of the dense lane, against the exact 10 nearest rows the subject
  may read. The truth is a scan in numpy over the stored 16-bit values (the
  `exact` search is capped at 100,000 rows, `MAX_EXACT_ROWS`), 100 queries per
  subject. The reader's own `POST /recall` (10 samples, key with `admin`) is
  reported next to it for the 100% subject.
- **Cold.** A fresh `reader serve` process on an empty mirror, whose first
  query is measured with the reader's own request and byte counters. The
  operating system page cache is **not** dropped, and storage is local, so a
  request has no network time: the request counts and bytes carry over to an
  object store, the seconds do not.

## Machine and build

- Intel Core i9-12900H, 20 logical CPUs, 31 GiB RAM, Linux 7.0.0-43-generic,
  an XFS volume (`/dev/loop0`, `noatime`) holding the storage and the reader
  mirror. Shared host: other jobs (CI runs of other checkouts) ran on it during
  the measurements, so absolute times carry that noise.
- Commit `fbcc57aaf48e472b34dcd6cc06a721417a054f9e` of `fyrr-ai/main`, release
  profile (fat LTO), `fyrr 0.1.1 (full build)`, binary SHA-256
  `15d23555f071a9724fc8d7cb7232bdc1c5f8d7a9c9c4fd4d40db7b583fc65eb9`.
  `scripts/bench-scale` is the one this change adds.
- Writer: `writer.threads = 2`, and `taskset -c 0-3` for `writer serve` and
  `writer maintain`, as the host requires. Reader: no pinning. The reader
  mirror is limited to 8 GiB (`mirror_max_bytes`) and used 1,661 MiB.
- `reader.memory_max_bytes` in three arms: the default **2304 MiB**, **6144 MiB**
  and **8192 MiB**.

## Commands

```sh
PATH=$HOME/.cargo/bin:$PATH fyrr-cargo build --release
# load, maintain (default 5 min step, rerun once), then both reader arms
scripts/bench-scale --rows 3000000 --queries 100 --native-recall 10 --keep \
  --dir /mnt/fetchcraft-builds/tmp/bench-scale
# the same namespace, queries only (state A below)
scripts/bench-scale --reuse DIR --queries 100 --native-recall 10
# a third maintain run with step_max_duration = 1h, then queries (state B)
scripts/bench-scale --reuse DIR --maintain --maintain-step 1h \
  --queries 100 --native-recall 10
```

The query phase of the first command never got a reader: the default budget is
refused (below), so I stopped it after the load and maintenance, and the later
two commands are the runs reported. The
queries of state A ran in two passes of the script that differ only in how the
isolated admission is estimated (the second query of an identity as baseline
there, the median of its fourth to sixth query in state B). The raw JSON of
every run is kept privately on the build host under
`/mnt/fetchcraft-builds/tmp/bench-scale-results/`; the namespace was deleted
afterwards.

## Write path and index build

60 requests of 50,000 rows, 3,000,000 rows in all, on `writer serve`:

| Measure | Result |
| --- | --- |
| Answer time of a 50,000-row request | median 63.5 s, minimum 55.3 s, maximum 81.7 s; 58.7 s for the first, 74 s for the last |
| Sum of the 60 answers | 3,863 s (64.4 min), 776 rows/s |
| Writer peak RSS (`VmHWM`) | 2,287 MiB |
| Storage after the load | 3.81 GB |
| Index segments after the load | 60 for each of the 12 indexes (`vector_idx`, `fts_idx` and the ten scalar ones); 60 fragments |

Each request is its own publication, so each leaves one more segment of every
index. A request time includes the segment's vector and full-text build.

`writer maintain` with the default `writer.maintenance.step_max_duration` (5
minutes) **failed twice** with `maintenance_step_deadline`, after 331 s and
313 s. Between them the ten scalar indexes and `fts_idx` were merged to one
segment each, `vector_idx` stayed at 60 (storage 4.08 GB). The failure happened
and was repeated once, as planned. A third run with `step_max_duration = 1h`
(one extra run beyond the plan, to measure the merged state) **succeeded in
2,147 s (35.8 min)** at a peak RSS of 1,509 MiB (sampled `VmHWM`), merging
`vector_idx` to one segment (storage 4.18 GB). The peak RSS of the two failed
runs (1,476 MiB and 932 MiB) is the `wait4` figure of the first pass of the
script, which can include the driver's own pages; the 1,509 MiB is sampled from
`/proc`.

Two states are therefore measured: **A**, as a namespace written in 50,000-row
requests is left (60 vector segments, everything else merged), and **B**, after
the vector merge (one segment).

## Reader memory

The default budget does not serve this namespace. `reader serve` exits with
`preparation_budget_impossible` before it answers `/readyz`:

| `reader.memory_max_bytes` | State A | State B |
| --- | --- | --- |
| 2304 MiB (default) | refused, minimum 6,025,707,520 B (5,746 MiB) | refused, minimum 5,994,070,016 B (5,716 MiB) |
| 6144 MiB | serves | serves |
| 8192 MiB | serves | serves |

That is the admission rule of the accounting (two warm claims and the native
cache minima, [tuning](../tuning.md)), as `tuning.md` predicts for 1,000,000
rows and more, now measured at 3,000,000. The resident set is far below it:

| Reader process | Peak RSS (`VmHWM`) |
| --- | --- |
| Started, `/readyz` answered | 38-40 MiB |
| After 900 queries (dense, full-text, hybrid), one subject | 386-479 MiB |
| After `/recall` with 10 exact scans over 3,000,000 rows (100% subject) | 585-590 MiB (A), 1,215-1,228 MiB (B) |

The reader mirror held 1,661 MiB after the queries. The reader answers
`/readyz` in 0.1 s: the namespace is opened lazily, with 39-96 storage requests
and 4-63 MiB, and the first query reads the index files (below).

## Latency, warm

p50 / p95 / p99 in milliseconds after the first query of each lane on the
process, 100 queries, no errors. The 8192 MiB arm; the p50 of the 6144 MiB arm
is within 15% of it on every cell (100% dense lane: 65.7 against 66.1 ms in
state A, 16.1 against 17.8 ms in B).

| Subject | Lane | State A (60 vector segments) | State B (merged) |
| --- | --- | --- | --- |
| 100% | dense | 66 / 73 / 80 | 18 / 44 / 52 |
| 100% | full-text | 27 / 72 / 89 | 30 / 82 / 156 |
| 100% | hybrid | 78 / 98 / 114 | 31 / 73 / 89 |
| 10% | dense | 65 / 72 / 74 | 35 / 95 / 246 |
| 10% | full-text | 175 / 362 / 379 | 170 / 366 / 831 |
| 10% | hybrid | 176 / 368 / 399 | 170 / 358 / 387 |
| 1% | dense | 94 / 106 / 121 | 78 / 87 / 92 |
| 1% | full-text | 40 / 95 / 106 | 35 / 86 / 92 |
| 1% | hybrid | 97 / 110 / 127 | 84 / 97 / 102 |

(The 8192 MiB arm of state B has outliers at p99, for example 831 ms for the 10%
full-text lane; the 6144 MiB arm of the same state reads 378 ms. With 100
queries a p99 is the maximum.)

Reading it:

- Merging the vector index cuts dense latency of the 100% subject from 66 to
  17 ms and of the 10% subject from 65 to 35 ms: a query probes one segment
  instead of 60. The 1% subject changes little (94 to 78 ms).
- The **10% subject's full-text and hybrid lanes are 4 to 6 times slower** than
  the 100% and the 1% subject (p50 170 ms against 28-35 ms, p95 about 360 ms).
  The lexical lane has to filter by a set that is neither small nor the whole
  corpus. This does not depend on the merge.
- The first query of every lane, which is the cold case, is in the next section.

## Cold first query and admission (#106)

A fresh reader, empty mirror, one subject per reader process. Request and byte
counts are the deltas of `fyrr_reader_object_store_requests_total` and
`fyrr_reader_object_store_bytes_total` around that query; `/readyz` came before
and is not included (39-96 requests, 4-63 MiB).

| Subject | State | First dense query | Storage requests | MiB read | First full-text (requests, MiB) | First hybrid (requests, MiB) |
| --- | --- | --- | --- | --- | --- | --- |
| 100% | A | 1.18-1.22 s | 739 | 1,094-1,121 | 0.09-0.10 s (853-941, 80-89) | 0.08 s (815-818, 75-76) |
| 10% | A | 1.28-1.32 s | 705-745 | 1,125-1,138 | 0.40 s (1,198-1,507, 400-401) | 0.32-0.33 s (63-95, 94-121) |
| 1% | A | 1.19-1.58 s | 580-743 | 1,107-1,254 | 0.08-0.09 s (444-470, 78-81) | 0.10-0.11 s (514-535, 91-97) |
| 100% | B | 1.21-1.38 s | 26,241-26,393 | 1,315-1,317 | 0.09-0.11 s (862-868, 82-87) | 0.05 s (754, 43-44) |
| 10% | B | 1.39-1.44 s | 35,782 | 1,545-1,606 | 0.38-0.40 s (958-1,141, 384-386) | 0.33 s (1-206, 37-116) |
| 1% | B | 1.69-1.73 s | 46,774 | 1,951-1,964 | 0.08-0.09 s (285-306, 74-81) | 0.09 s (306-322, 84) |

The merged index is read in small pages: **26,000 to 47,000 requests for 1.3 to
1.9 GiB** on the first dense query, against about 740 requests for 1.1 GiB when
the index has 60 segments. On local storage that costs 1.2-1.7 s; on an object
store every request has a round trip (this is where #103 and #104 apply) and
the figure is not measured here.

**Admission** is the part of that first query that builds the subject's
permission set. On one cold reader the first subject loads the files and builds
its admission together (the first row of each order below); the subjects after it
find the files in the mirror, so their first query is the admission alone. The
telemetry's `stage_s3_requests.admission` is 0 in every first query of every run.

| Subject | State | First query on warm files | Later queries | Difference | Storage requests, bytes |
| --- | --- | --- | --- | --- | --- |
| 100% | A | 161-184 ms | 70-74 ms | **87-117 ms** | 33-257 req, 160-178 MB |
| 10% | A | 132-134 ms | 65-67 ms | **65-69 ms** | 1-51 req, 104-136 MB |
| 1% | A | 168 ms | 96-100 ms | **67-73 ms** | 273-292 req, 173-175 MB |
| 100% | B | 612-722 ms | 479-563 ms | 93-193 ms | 0 req |
| 10% | B | 287-353 ms | 285-351 ms | 2 ms | 0 req |
| 1% | B | 301-302 ms | 107-142 ms | 160-194 ms | 1-2,981 req, 73-263 MB |

State A is clear: **admission at 3,000,000 rows costs about 0.1 s for the 100%
subject and 0.07 s for the 10% and 1% subjects, on warm files**, a far cry from
the 300 s `MAX_REQUEST_DEADLINE`. The first query of a cold reader costs 1.1-1.4
s in all, nearly all of it reading the index files. In state B the figures are
less clean (the second and later queries of a subject that is not the first on
its reader are slow, see the findings), so the differences there are an upper
bound at best. Neither state shows an admission that grows toward the deadline:
**the per-bind corpus pass is not the bottleneck at 3,000,000 rows.**

*Extrapolated, not measured:* if the admission cost grows linearly with the
admitted rows, 30,000,000 rows would take about 1 s warm for the 100% subject.

## Recall

Recall@10 of the dense lane against the exact 10 nearest rows the subject may
read, 100 queries per subject, all arms (the arms agree):

| Subject | State A | State B |
| --- | --- | --- |
| 100% | 0.869 (38 of 100 queries under 0.9, lowest 0.40) | 0.972 (2 under 0.9, lowest 0.80) |
| 10% | 1.000 | 1.000 |
| 1% | 1.000 | 1.000 |

The 10% and 1% subjects return exactly the truth: their selection is small
enough that the reader scans it exactly instead of probing the index. The 100%
subject, whose dense lane uses the index, loses recall when the index has 60
segments and recovers most of it after the merge. All three use the default
`nprobes` (256 per segment) and 300 candidates.

The reader's own `POST /recall` (10 samples taken from the stored rows, 100%
subject, 45-49 s each run): state A `recall_at_10` **0.95**, `recall_at_75`
**0.712**, `approximate: true`. In state B it answered `approximate: false`, so
at least one sample was answered by an exact scan, and `recall_at_10` 1.0 there
measures no index (see the route's [documentation](../http-api.md#recall)). The
numpy scan above is the recall figure of state B.

## Findings

These are candidates for their own issues; none is fixed here.

1. **The default `reader.memory_max_bytes` (2304 MiB) refuses a 3,000,000-row
   namespace**, minimum about 5.7 GiB, while the process stays under 0.5 GiB
   resident (1.2 GiB during `/recall`). An operator gets
   `preparation_budget_impossible` and no way to know the number except from the
   log.
2. **`writer maintain` cannot merge a 60-segment vector index within the default
   5-minute step**: two runs failed with `maintenance_step_deadline` (331 s and
   313 s); it takes 2,147 s. `writer serve` runs maintenance only above 64
   segments, so a namespace loaded in 50,000-row requests sits at 60 segments
   with 0.87 recall@10 and 4 times the dense latency.
3. **The merged vector index is read in about 26,000-47,000 small requests on
   the first query** (1.3-1.9 GiB), 35 to 65 times the requests of the
   unmerged one for a similar number of bytes.
4. **Full-text and hybrid queries of a subject that reads 10% of the rows take
   about 170 ms (p95 360 ms)**, 4 to 6 times those of a subject that reads all or
   1%.
5. **Queries of a subject that is not the first one on a reader are slow in
   state B**: on a reader that served the 1% (or 10%) subject first, the 100%
   subject's dense queries take 480-560 ms for six queries in a row, against 16
   ms on a reader that serves only that subject. The cause is not known; the
   isolated-admission rows above are affected by it.
6. **`POST /recall` answers by an exact scan at this size** (`approximate:
   false`) and takes 4-5 s per sample, so it cannot report the index recall of
   a merged 3,000,000-row namespace.

## Not measured

- **Other dimensions.** Only 128 dimensions. A 1,024-dimension vector is 8
  times the stored bytes and the cold bytes; the row-state memory formula does
  not depend on it.
- **Real data.** Synthetic clusters and a synthetic Zipf text. Recall on real
  embeddings and the cost of real query terms may differ.
- **Object stores.** Local storage only: no network latency in any cold figure,
  no S3 request cost; the request counts are storage-layer counts.
- **Dropped page cache.** The operating system cache was warm after the load.
- **More rows.** 3,000,000 only. Nothing is measured at 10,000,000 (#133) or
  above; the 100,000-row run of the pilot and this one give no more than two
  points, and no size limit of a namespace (#194) follows from them. The 30
  million figure above is extrapolated.
- **Writes of other shapes.** Only 60 requests of 50,000 rows, each its own
  publication; coalesced commit groups of up to 1,000,000 rows, a
  `writer maintain` at 64 segments, `dense_partition_rows` or `dense_rq_bits`
  other than the defaults, and re-partitioning are not measured.
- **Load.** One client, no concurrency, no throughput, no rate limit.
- **The other lanes' recall.** Full-text and hybrid have no recall figure; the
  open-namespace fast path (`open = true`) and `reader.memory_max_bytes` between
  2304 and 6144 MiB are not measured.
- **Admission after a version change.** Only the first admission of a reader;
  the rebuild after a publication is not measured.
