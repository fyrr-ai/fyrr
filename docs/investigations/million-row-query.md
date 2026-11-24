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
two commands are the runs reported. The two query runs differ in one detail of
the script: the "later queries" that the comparison under
[Cold first query](#cold-first-query-and-admission-106) subtracts are the
second query in state A and the median of the fourth to sixth in state B; the
script in this change is the second. The raw JSON of every run is kept
privately on the build host under
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
313 s. After the two runs the ten scalar indexes and `fts_idx` were merged to one
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

The p99 of 100 queries is the second slowest. The 8192 MiB arm of state B has
outliers: the 10% full-text lane has p99 831.2 ms and a maximum of 4,016.9 ms
(the 6144 MiB arm of the same state: p99 377.6 ms, maximum 437.7 ms), and the
100% full-text lane a maximum of 4,904.4 ms (6144 MiB arm: 106.8 ms). Other
jobs ran on the host, so I take these as noise of a shared machine, but I did
not repeat the runs to show it.

Reading it:

- Merging the vector index cuts dense latency of the 100% subject from 66 to
  18 ms and of the 10% subject from 65 to 35 ms: a query probes one segment
  instead of 60. The 1% subject changes little (94 to 78 ms).
- The **10% subject's full-text lane is 4 to 7 times slower** than the 100%
  and the 1% subject (p50 170-175 ms against 27-40 ms, p95 about 360 ms) in both
  states. Its hybrid lane (p50 170-176 ms) is 2.3 and 1.8 times the 100% and
  1% subjects' in state A (78 and 97 ms), and 5.4 and 2.0 times in state B (31
  and 84 ms). I did not look into the cause.
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
| 1% | B | 1.69-1.73 s | 46,774 | 1,951-1,964 | 0.08-0.09 s (285, 74-81) | 0.09 s (306-322, 84) |

The merged index is read in small pages: **26,000 to 47,000 requests for 1.3 to
1.9 GiB** on the first dense query, against about 580-745 requests for 1.1-1.2 GiB when
the index has 60 segments. On local storage that costs 1.2-1.7 s; on an object
store every request has a round trip (this is where #103 and #104 apply) and
the figure is not measured here.

**Admission was not isolated.** The first query of a subject builds that
subject's permission set, and the search response exposes no per-stage time
(`stage_us` is omitted), so no time is attributed to admission. The telemetry
attributes 0 storage requests and 0 bytes to its `admission` stage in every
first query of every run, while the reader's own storage counters moved by tens
to tens of thousands of requests in the same queries (the table above), so
those are not separable either. The only statement these runs support is that
the whole cold first query, which contains the admission, took **1.2-1.6 s in
state A and 1.2-1.7 s in state B** for every subject, so at 3,000,000 rows the
admission of one subject took no longer than that, far below the 300 s
`MAX_REQUEST_DEADLINE`. It is not a measure of admission, and nothing is
measured at a larger size.

A diagnosis, not a bound and not an admission time: on one cold reader I ran the
first query of one subject (which reads the files) and then the first query of
each other subject, and compared the latter with the same subject's later
queries on that reader (the second query in state A, the median of the fourth
to sixth in state B). Later queries have a latency of their own, and the first
query reads other things too, as the request counts show:

| Subject | State | First query after the files were read | Later queries | Storage requests, MiB read in the first query |
| --- | --- | --- | --- | --- |
| 100% | A | 161-184 ms | 68-74 ms | 33-257, 153-170 |
| 10% | A | 132-134 ms | 65-67 ms | 1-51, 99-129 |
| 1% | A | 168-169 ms | 96-100 ms | 273-292, 165-167 |
| 100% | B | 612-722 ms | 479-563 ms | 0, 0 |
| 10% | B | 287-353 ms | 285-351 ms | 0, 0 |
| 1% | B | 301-302 ms | 108-142 ms | 1-2,981, 69-251 |

In state B the later queries are themselves slow for the 100% and 10%
subjects (finding 5), so the baseline is not a clean warm query there.

## Recall

Recall@10 of the dense lane against the exact 10 nearest rows the subject may
read, 100 queries per subject, all arms (the arms agree):

| Subject | State A | State B |
| --- | --- | --- |
| 100% | 0.869 (38 of 100 queries under 0.9, lowest 0.40) | 0.972 (2 under 0.9, lowest 0.80) |
| 10% | 1.000 | 1.000 |
| 1% | 1.000 | 1.000 |

The 10% and 1% subjects returned exactly the truth in these runs (I did not
check whether the reader scanned their selection exactly or probed the index).
The 100% subject loses recall when the index has 60 segments and recovers most of
it after the merge. All three use the default
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
   namespace**, minimum about 5.6 GiB (5,746 MiB), while the process stays under 0.5 GiB
   resident (1.2 GiB during `/recall`). An operator gets
   `preparation_budget_impossible` and no way to know the number except from the
   log.
2. **`writer maintain` cannot merge a 60-segment vector index within the default
   5-minute step**: two runs failed with `maintenance_step_deadline` (331 s and
   313 s); it takes 2,147 s. `writer serve` runs maintenance only above 64
   segments, so a namespace loaded in 50,000-row requests sits at 60 segments
   with 0.87 recall@10 and 4 times the dense latency. (Rerun after #323:
   see the last section.)
3. **The merged vector index is read in about 26,000-47,000 small requests on
   the first query** (1.3-1.9 GiB), 35 to 80 times the requests of the
   unmerged one (580-745) for a similar number of bytes. (Rerun after #322:
   see the last section.)
4. **Full-text and hybrid queries of a subject that reads 10% of the rows take
   about 170 ms (p95 360 ms)** in the full-text lane (4 to 7 times those of a
   subject that reads all or 1%) and in the hybrid lane (1.8 to 5.4 times).
5. **Queries of a subject that is not the first one on a reader are slow in
   state B**: on a reader that served the 1% (or 10%) subject first, the 100%
   subject's dense queries take about 440-600 ms for six queries in a row (one
   of 1,086 ms), against 16-18 ms p50 on a reader that serves only that
   subject. The cause is not known; the comparison of first and later queries above
   is affected by it.
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
  above. One size gives no slope and no size limit of a namespace (#194).
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

## Rerun after tiered merging (#323) and whole-file index download (#322)

The same namespace shape was written and measured again with the defaults, on
`fyrr-ai/main` at `9ab044b848de0ed6bef739e218ea78f162a6721a`, which holds the
tiered vector merge of [#319](https://github.com/fyrr-ai/fyrr/issues/319)
(#323) and the whole-file download of the vector index files of
[#320](https://github.com/fyrr-ai/fyrr/issues/320) (#322). Release profile
(fat LTO), binary SHA-256
`09599886eb311bc3c24230648ad0d7c5ba6a5cc25201f056695da7389ae142ed`, same machine
and storage as above, `writer.threads = 2` and `taskset -c 0-3` for the load
and for every `writer maintain`. **No `step_max_duration` override** (default 5
minutes) and no change of any other setting; readers at the default
`reader.memory_max_bytes` (2304 MiB) and at 6144 MiB.

```sh
scripts/bench-scale --rows 3000000 --queries 100 --native-recall 10 --keep \
  --memory 2415919104 6442450944 --dir /mnt/fetchcraft-builds/tmp/bench-scale
# a second pass of the queries on the same namespace, 6144 MiB only
scripts/bench-scale --reuse DIR --queries 100 --native-recall 10 --memory 6442450944
```

`scripts/bench-scale` now repeats `writer maintain` while a run fails or still
merged something (at most 8 runs) and keeps the telemetry line of every merge
step. **The host was busy**: other agents' builds and CI ran during the whole
measurement (load average 2.4-13 on 20 CPUs), so the latencies below carry that noise; the second
pass shows how much. The raw JSON is kept privately on the build host.

### `writer maintain` with the defaults (#319)

60 requests of 50,000 rows, as before (sum of the answers 4,711 s, median
79.8 s, writer peak RSS 2,326 MiB; the host was slower than for the first
load). 60 vector segments and 60 fragments afterwards, one `event=maintained`
of `writer serve` and no merge line from it. Then `writer maintain` four times,
every run exit code 0, no `maintenance_step_deadline`, no
`index_merge=skipped`:

| Run | Seconds | Peak RSS (MiB) | Merge steps | Longest step | Fragments, vector segments after |
| --- | --- | --- | --- | --- | --- |
| 1 | 50.0 | 1,791 | 26: 11 of 3,000,000 rows, 15 of 200,000 | 14.3 s | 60, **15** |
| 2 | 192.1 | 1,863 | 26: 11 of 3,000,000 rows, 15 of 200,000 | 90.4 s | 30, 15 |
| 3 | 207.1 | 1,803 | 11 of 3,000,000 rows | 109.2 s | 15, 15 |
| 4 | 38.0 | 1,085 | none | none | 15, 15 |

Total 487 s (8.1 minutes), against 2,147 s for the one-step merge of #317.
Reading it:

- **The vector index is down to 15 segments in the first run, as #323
  predicted, and each of its 15 steps took 0.1-0.3 s** (four 50,000-row
  segments joined, no partition rebalancing). The other 11 indexes were merged
  in 11 steps of 0.0-14.3 s in that run.
- **It converges without an override, but in four invocations**: the fourth
  does nothing. Runs 2 and 3 merged again without changing a segment count: the
  fragments went 60, 30, 15, and the merge steps of the later runs follow that
  (I infer a compaction step per run; the steps do not name their index, so I
  could not say which of the 11 took 14, 90 and 109 s). An operator that runs
  `writer maintain` once is left with 15 vector segments and 60 fragments
  (the first run), then with the fragments halved by each later run.
- Storage after the four runs was 8.63 GB (3.82 GB after the load; retained old
  versions), against 4.18 GB after the merge of #317.

### Cold first dense query (#320)

A fresh reader on an empty mirror, one subject per process, as before. Both
budgets: 2304 MiB (default) again **refuses** the namespace with
`preparation_budget_impossible`, minimum 6,060,617,728 B (5,780 MiB)
([#318](https://github.com/fyrr-ai/fyrr/issues/318)); 6144 MiB serves it. The
two passes (one reader process per subject and pass):

| Subject | First dense query, pass 1 | First dense query, pass 2 |
| --- | --- | --- |
| 100% | 1.86 s, 981 requests, 914 MiB | 1.08 s, 976 requests, 512 MiB |
| 10% | 2.32 s, 975 requests, 730 MiB | 4.89 s, 995 requests, 474 MiB |
| 1% | 1.83 s, 971 requests, 702 MiB | 0.83 s, 969 requests, 613 MiB |

**969-995 storage requests for 474-914 MiB, against 26,241-46,774 requests for
1,315-1,964 MiB in #317's one-segment state** (and 580-745 for 1,094-1,254 MiB
in its 60-segment state). The state is not the same (15 segments here), but the
page-by-page reads are gone. The first full-text query then took 1,140-5,330
requests (106-811 MiB) and the first hybrid one 1,810-6,122 (40-133 MiB). The
seconds are as noisy as the rest.

### Warm latency and recall at 15 segments

p50 / p95 / p99 in milliseconds, 100 queries per cell, no errors, 6144 MiB
reader, the two passes, next to the 60-segment and one-segment states of #317
(8192 MiB arm) for the dense lane:

| Subject | Lane | Pass 1 | Pass 2 | #317, 60 segments | #317, 1 segment |
| --- | --- | --- | --- | --- | --- |
| 100% | dense | 51 / 69 / 94 | 59 / 137 / 257 | 66 / 73 / 80 | 18 / 44 / 52 |
| 100% | full-text | 29 / 162 / 260 | 35 / 91 / 112 | 27 / 72 / 89 | 30 / 82 / 156 |
| 100% | hybrid | 66 / 150 / 700 | 54 / 70 / 82 | 78 / 98 / 114 | 31 / 73 / 89 |
| 10% | dense | 72 / 1,407 / 2,892 | 50 / 750 / 1,266 | 65 / 72 / 74 | 35 / 95 / 246 |
| 10% | full-text | 210 / 498 / 571 | 188 / 900 / 1,551 | 175 / 362 / 379 | 170 / 366 / 831 |
| 10% | hybrid | 235 / 473 / 566 | 245 / 757 / 2,398 | 176 / 368 / 399 | 170 / 358 / 387 |
| 1% | dense | 117 / 696 / 1,691 | 97 / 118 / 125 | 94 / 106 / 121 | 78 / 87 / 92 |
| 1% | full-text | 45 / 108 / 120 | 40 / 97 / 100 | 40 / 95 / 106 | 35 / 86 / 92 |
| 1% | hybrid | 106 / 136 / 249 | 97 / 112 / 118 | 97 / 110 / 127 | 84 / 97 / 102 |

Recall@10 of the dense lane against the exact scan (100 queries per subject),
the same in both passes:

| Subject | 15 segments | #317, 60 segments | #317, 1 segment |
| --- | --- | --- | --- |
| 100% | **0.850** (48 of 100 queries under 0.9, lowest 0.50) | 0.869 | 0.972 |
| 10% | 1.000 | 1.000 | 1.000 |
| 1% | 1.000 | 1.000 | 1.000 |

The reader's own `POST /recall` answered `approximate: false` again (an exact
scan, 58-65 s for 10 samples), so it reports 1.0 and measures no index. Peak
RSS of the reader: 35-37 MiB at `/readyz`, 372-432 MiB after the queries,
1,057-1,103 MiB after `/recall`; `/readyz` answered in 0.1-0.16 s (9.5 s once,
in the second pass, on the busy host).

What this says:

- **Tiering fixes the maintenance, not the query.** At 15 segments the 100%
  subject's dense p50 is 51-59 ms, about 3 times the one segment of #317 (18
  ms) and close to the 60 segments (66 ms); the estimate of 29 ms in #323
  (interpolating the two) was not reached. Recall@10 is **0.850**, below even
  the 60-segment state (0.869) and far below the merged one (0.972).
- The tails are bad and noisy: the 10% subject's dense lane has p95 of 750-1,407
  ms in both passes (72-95 ms in #317), and single queries of 5-9 s occurred
  (pass 2: 10% full-text maximum 9,252 ms, hybrid 5,097 ms; pass 1: 1% dense
  maximum 4,861 ms). Both passes ran with other jobs on the host, so I cannot
  separate the host from the index here; the dense lane of the 1% subject is
  back at 97-117 ms p50 in both.
- Finding 5 of #317 (a subject that is not the first on a reader) shows again
  in one of the two orders of the isolated runs: the 100% subject, after the
  10% and the 1% subject, took 400-1,250 ms for its first six queries, and
  69-135 ms in the order where it came after the 1% subject alone (51-59 ms
  p50 on a reader of its own).

### Not measured in this rerun

- A one-segment namespace at the defaults: the tiered merge stops at 15
  segments here, and this rerun did not run `writer maintain` with a larger
  step budget to get there, so the comparison with #317's one segment is across
  two builds and two states.
- The effect of the whole-file download on a reader with a small mirror, on an
  object store (round trips) or with the page cache dropped; only requests and
  bytes of the first query on local storage.
- A repeat on a quiet host. Everything else in "Not measured" above holds.
