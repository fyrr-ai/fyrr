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
