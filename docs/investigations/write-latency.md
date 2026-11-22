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
i9-12900H (20 logical CPUs, 31 GiB), Linux 7.0.0-43. The S3 fixture and real S3
were **not measured**; the script runs them with `--storage fixture` and
`--storage s3`. Two complete runs agree; the table is the second.

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
