# Write latency and write-to-searchable time

`scripts/bench-write` drives the real `fyrr writer serve` and `fyrr reader
serve` executables over HTTP on one storage. Python only sends the requests and
scores the timings; every number comes from those executables on the real path.

## Protocol

- Every row carries its own 128-dimension vector, so no embedding call is in the
  path. Rows have a short text and no attributes; the namespace is open.
- One fresh namespace per batch size, each created, and opened by the reader,
  before anything is timed. Rows are new on every write (no replacement).
- Batch sizes and writes per size: 1 row × 1,000 writes, 1,000 rows × 50 writes,
  50,000 rows × 5 writes. The p99 of fewer than 100 writes is their maximum.
- `write` is the time until the write is answered, which is when the served head
  holds it. `read` is a `strong` fetch of the batch's last row, sent when the
  answer arrives, until the row is returned; `total` is their sum. The fetch
  looks the row up by its key, so `read` is the cost of the strong version check
  and a key lookup, not of a ranked search.
- Object-store operations are those that the writer's and reader's storage
  wrappers record (`FYRR_OBJECT_STORE_DIAGNOSTICS`), counted by method per write
  and per read. On local storage they are calls of the storage layer, not
  network requests.
- `fragments` and `index_segments` come from `fyrr writer status` after the 1,000
  single-row writes.
- `receive` splits the 50,000-row write: the same body refused with `403` after
  it was read and scanned, and with `400 vector_invalid` after the 49,999 rows
  before the last were bound.
- Writer threads are 2 (`writer.threads`); the commit coalescing timer is its
  default, a write wakes the committer itself.

Run `scripts/bench-write --storage local|fixture|s3`; it prints one JSON
document with the machine, storage and versions.

## Result: local storage

Measured with the release build of this change (`fyrr 0.1.1`, full build) on **local storage** (`file://`, an XFS volume), on a 12th Gen Intel Core
i9-12900H (20 logical CPUs, 31 GiB), Linux 7.0.0-43. Two complete runs agree;
the table is the second. The S3 fixture is measured below; real S3 is not.

| Rows per write | Writes | `write` p50 / p90 / p99 (ms) | `read` p50 / p90 / p99 (ms) | `total` p50 / p90 / p99 (ms) |
| ---: | ---: | --- | --- | --- |
| 1 | 1,000 | 438 / 696 / 29,785 | 110 / 210 / 351 | 549 / 912 / 29,944 |
| 1,000 | 50 | 560 / 778 / 880 | 85 / 143 / 163 | 645 / 925 / 1,043 |
| 50,000 | 5 | 47,462 / 50,431 / 50,431 | 57 / 120 / 120 | 47,519 / 50,517 / 50,517 |

The p99 of 50 and of 5 writes is their maximum. The first run gave 416 / 774 /
35,597 ms (`write`, 1 row), 602 / 792 / 966 ms (1,000 rows) and 57,263 / 80,402
ms (50,000 rows, p50 / maximum).

- **The tail of single-row writes is maintenance.** 16 of the 1,000 writes took
  over 10 s (maximum 43.7 s); `writer serve` ran maintenance 15 times in that
  phase, and a write waits for the committer's lease meanwhile. The 1,000- and
  50,000-row phases ran it 0 times, and the 50,000-row writes took 47–50 s
  without it.
- **Object-store operations per write**, as the writer's storage wrapper
  records them (local storage: calls of the storage layer, not network
  requests): 1 row: 1,831 `GET`, 53 `HEAD`, 28 `LIST`, 65 `PUT`; 1,000 rows:
  719 `GET`, 39 `HEAD`, 18 `LIST`, 36 `PUT`; 50,000 rows: 1,260 `GET`, 43
  `HEAD`, 19 `LIST`, 36 `PUT` and 2 multipart uploads. A strong fetch costs the
  reader 26–52 `GET`. These counts include the writer's maintenance work and
  its polling of the head while it waits for the answer.
- **After 1,000 single-row writes** the served version has 168 data files
  (fragments) and 41 segments in each of its 12 indexes.
- **Request intake is a small share of the write.** The 50,000-row body (about
  62 MB, 128 dimensions) was refused with `403` after it was received and
  scanned in 81 ms (p50 of 3) and with `400 vector_invalid` after its first
  49,999 rows were bound in 304 ms, against 47,462 ms for the write. The 304 ms
  is a diagnostic upper bound for request intake, not a measurement of JSON
  number parsing: it includes the transfer, authorization, state reads, parsing
  and row validation. Intake is under 1 % of the write, so no binary vector
  encoding issue is opened for now.
- `read` is a strong fetch by key after the answer: the head check and a key
  lookup, 85 ms at p50 for 1,000 rows. It is not the latency of a ranked search.

## Result: S3 fixture (RustFS on local disk)

**This is not real S3.** The fixture is one RustFS container (image
`rustfs/rustfs@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff`,
memory limit 4 GiB) on the same host as the writer and the reader, reached over
loopback HTTP, with its data on the same XFS volume. It has no network latency,
no S3 request pricing and none of S3's throttling, so these numbers show the
cost of the object-store *protocol* (many small requests) against a fast local
server, and not what a deployment on S3 will see.

Measured with the release build (`fyrr 0.1.1`, full build, commit `5ff777b5`
plus the script changes of this change) via `scripts/s3-fixture start` and
`scripts/bench-write --storage fixture`, on the same machine as the local run
(12th Gen Intel Core i9-12900H, 20 logical CPUs, 31 GiB, Linux 7.0.0-43). One
complete run; the whole run took about 5.5 hours.

| Rows per write | Writes | `write` p50 / p90 / p99 (ms) | `read` p50 / p90 / p99 (ms) | `total` p50 / p90 / p99 (ms) |
| ---: | ---: | --- | --- | --- |
| 1 | 1,000 | 8,403 / 15,795 / 445,413 | 156 / 291 / 418 | 8,692 / 15,986 / 445,507 |
| 1,000 | 50 | 3,592 / 4,415 / 6,524 | 122 / 188 / 204 | 3,732 / 4,595 / 6,646 |
| 50,000 | 5 | 53,983 / 57,397 / 57,397 | 85 / 149 / 149 | 54,068 / 57,545 / 57,545 |

The p99 of 50 and of 5 writes is their maximum.

- **A single-row write is about 19 times slower than on local storage** (p50
  8.4 s against 0.44 s) and **a 1,000-row write about 6 times slower** (3.6 s
  against 0.56 s). A 50,000-row write is 14 % slower (54 s against 47 s): its
  cost is the work on the rows, not the requests. A strong `read` costs
  1.4–1.5 times the local one.
- **Single-row writes did not stay inside the writer's answer deadline.** 7 of
  the 1,000 writes got no answer in 600 s (`503 outcome_unknown`, the writer's
  `ANSWER_DEADLINE`); they are counted at 600 s in the percentiles and have no
  read, so the p99 of 445 s is the 10th-longest write. 460 writes took over 10
  s. `writer serve` ran maintenance 16 times in that phase, and 14 of the writes
  over 10 s ended while a maintenance run had completed. The committer runs
  maintenance in its own loop, so a write waits for it. Two earlier attempts of
  this run stopped at the first such write (the second at the 576th) because
  the script then asserted `200`; the script now counts them and continues.
  The 1,000- and 50,000-row phases ran maintenance 0 times and had no timeout.
- **Object-store operations per write**, as the writer's storage wrapper records
  them: 1 row: 1,804 `GET`, 25 `HEAD`, 39 `LIST`, 61 `PUT`; 1,000 rows: 633
  `GET`, 13 `HEAD`, 22 `LIST`, 34 `PUT`; 50,000 rows: 1,223 `GET`, 12 `HEAD`, 23
  `LIST`, 34 `PUT` and 2 multipart uploads (8.2 parts). A strong fetch costs the
  reader 26–47 `GET`. Counts include maintenance and the writer's polling of the
  head while it waits for the answer. Each call is a request to the fixture; the
  hundreds of `GET`s per write are what separate 8.4 s from 0.44 s.
- **After 1,000 single-row writes** the served version has 162 data files and
  34 segments in each of its 12 indexes (local: 168 and 41).
- **Request intake** is again small: the 50,000-row body was refused with `403`
  after it was received and scanned in 113 ms and with `400 vector_invalid`
  after its first 49,999 rows were bound in 354 ms, against 53,983 ms for the
  write.

## Real S3: not measured

No credentials for a real S3 bucket are configured for this repository: the
`real S3:` rows of `scripts/ci` run against the same RustFS fixture
(`scripts/s3-fixture`, started by `scenario_setup` for `backend: s3`), and
neither the repository nor the environment of this run holds AWS credentials.
`scripts/bench-write --storage s3` is ready for it (`FYRR_BENCH_S3_URI` and the
AWS credential chain) but has not been run, so there is no real-S3 number here.
The fixture numbers above must not be read as one.
