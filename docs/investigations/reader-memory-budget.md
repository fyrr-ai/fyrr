# Reader memory budget investigation

This is a controlled diagnostic of the preserved 20,000-document release index,
not a new benchmark headline or a retrieval change. The original Lambda runs
changed both reader memory budget and function allocation; CPU entitlement and
runtime windows therefore differed as well. Their result variation does not
isolate a memory-budget effect.

## Protocol

Use the same release binary, immutable publication, identity and retrieval policy
in four fresh-process arms: 2304 MiB, 1536 MiB, 1536 MiB, 2304 MiB. Fix CPU affinity
and `LANCE_CPU_THREADS` to two, run the 250 literal questions in the same seed-1729
order, and preserve the default ten-second search deadline and candidate limits.
Each arm performs the same inventory check and first-question warmup. Keep all
TOML fields and the replay endpoint identical except `reader.memory_max_bytes`.

Freeze the 250 `voyage-4` query vectors once with `input_type = "query"` and 1024
dimensions. Replay them from an owned loopback service in every arm: no external
provider calls occur during the comparison. Vector capture has a separate
$0.10 cap and a persistent reservation ledger; uncertain attempts retain their
reservation. Run on an owned copy of the index so neither a writer nor mirror
activity can change the preserved source.

`scripts/reader-memory-ab` checks matching provider inputs/configurations and
retains per-arm readiness, inventory, raw ordered hit IDs and scores, response
metadata, stderr, and cross-arm/repeated-arm comparisons. It also records input
hashes and a host/process snapshot. Output belongs under `target/`; it is private
operator evidence rather than a committed corpus or credential artifact.

## Source analysis

`PreparationReservation::pool` subtracts startup RSS, headroom and the shared
128 MiB authorization-cache allowance from `reader.memory_max_bytes`. Warm and
cold preparation claims and native caches share the remaining pool. Candidate
limits are configured independently in `namespace.retrieval`.

If the warm claim does not fit, `PreparationReservation::acquire` can admit a cold
generation. Initial admission first requires space for two warm claims and
native cache minima; if that minimum cannot fit, startup instead refuses with
`preparation_budget_impossible`. `PreparedReader::dense` on a cold generation
bypasses prepared partition maps and
calls `native_dense_rows` with indexed search disabled. That exact KNN path can
produce a different candidate set from ANN. An undersized native cache alone
keeps indexed search; cache degradation is reported in readiness and telemetry.

`metadata.degraded` identifies skipped lanes, such as dense retrieval after a
query-embedding failure. It does not identify every cache/preparation fallback.
An empty degraded list alone therefore does not establish identical execution.
Check readiness, `generation_preparation` events, routing, dense/lexical candidate
counts, cutoff-tie counters, response truncation, and deadline/errors together.

Lexical cutoff ties have an explicit fetch bound and counter; dense rescoring also
has an explicit depth/tie bound. Neither is scaled directly by the memory setting.
A preparation-path switch, a failed memory reservation or a deadline can still
change the observed outcome. No ranking behavior is changed by this investigation.

## Result

The run used Linux x86-64, kernel 7.0.0-42-generic, on an Intel Core i9-12900H
with two fixed CPU IDs available to each reader. MemAvailable was 25,420,754,944
bytes at the start. No compiler ran during the comparison; the retained process
snapshot includes the existing idle reader and Docker services. Those services
were not changed or stopped.

The memory setting alone did not reproduce the Lambda ranking variation. All
four arms completed **250/250** queries. Ordered hit IDs, revisions and scores
matched exactly for every query, including the repeated same-budget arms. The
previously affected gold document for `qst_0231` stayed at **rank 10** throughout.

| Arm | Budget | Successful queries | Rank/score differences from arm 1 |
| --- | --- | --- | --- |
| A | 2304 MiB | 250/250 | 0 |
| B | 1536 MiB | 250/250 | 0 |
| B repeat | 1536 MiB | 250/250 | 0 |
| A repeat | 2304 MiB | 250/250 | 0 |

Each response used publication version 13, exact centroid routing, 300 dense
candidates and 201 lexical candidates. Both cutoff-tie counters were zero.
There were no errors, degraded lanes, hidden sources, stale versions, response
truncation or generation-degradation events in any arm. Both startup checks
reported `serving`, not a cold generation: native cache capacity was 160 MiB
against 128 MiB needed, and the preparation reservation was 40,521,728 bytes.
Their startup admission minima were about 505 MiB, well below either budget.
`warm_ready` was false in the startup-only check; it did not request prewarming.

The budget therefore did not bind the preparation/cache path or change candidate
limits in this experiment. Recall@10 was 0.9478723404, nDCG@10 0.8765120958 and
recall@50 0.9884633570 in every arm (235 questions with nonempty gold sets).
These are diagnostic results on identical frozen inputs, not new headline scores.
The original Lambda cause remains unresolved: its allocations also changed CPU
entitlement and execution windows, and its original query vectors were not saved.
No ranking or retrieval behavior is changed.

The preserved corpus had 19,999 searchable documents and **20,020 physical rows**;
one file was already quarantined in the preserved publication. This investigation
did not reingest or repair that index. The first startup populated the owned
mirror; later startups reused it. The balanced repeats produced the same ranking
and scores despite that difference, and no latency comparison is claimed.

Vector capture used ten calls and reported 9,120 input tokens. Its retained
reservation was **$0.025** against the approved **$0.10** cap; actual billed cost
is n/e. The four replay arms made zero external provider calls. Compilation and
CI started only after the quiet comparison and separate memory sizing finished.

## Evidence

The release was commit `38e7df7198c52f340da1737dca6af665acc69b96`, binary SHA-256
`9d9c76a6d0fe902535d7f66e5f387dd857a2de6be4f2edc3af4aa6629167640b`.
The frozen-vector artifact SHA-256 is
`a10fc108b6e811bc02443a8909d9116e7f0c984278282aea06db972a88a9aa08`.
The complete comparison SHA-256 is
`5a6637e79fecf2b9750b0f05b7547282dc6a1976dda208ad0c2ebe0bdbdb1b7e`.

Private evidence is retained under `target/investigation/memory-ab-results/`:
`driver.py` (the exact executed driver, before formatting), `identity.json`,
`host.json`, `comparison.json`, `summary.json`, and each arm's
`reader.toml`, `readiness.json`, `inventory.json`, `warmup.json`,
`responses.jsonl` and `stderr.log`. Vector capture and its reservation ledger
are under `target/investigation/frozen/`. None contains credentials.
