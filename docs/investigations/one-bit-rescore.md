# Dense recall after tiered maintenance

At 3 million rows the same 15 segments returned 0.823 and 0.996 recall@10, depending on how many approximate hits the reader rescored. The broader search cost 11 ms at the warm median for the unrestricted subject.

## Design

Keep the writer's bounded merges and the shared IVF model. On the built-in one-bit vector's partition-map path, take the integer square root of the number of selected rows as the initial exact rescore depth. Clamp it to the existing range of four to sixteen times the requested lane depth. This gives large selections more chances to recover a neighbor whose one-bit score ranked it too low. A fixed depth of 1,200 would do the same work for the unrestricted subject and unnecessary work for small selections. Explicit fixed and adaptive depths keep their configured behavior.

Lance 14.0.3 already supports wider searches, multi-bit codes and bounded same-model merges. A higher bit count requires encoding the data again. Per-tier rebalancing can produce distinct centroids that the next physical merge rejects. The same bounded-merge restrictions are present in [upstream Lance](https://github.com/lance-format/lance/blob/3187938685f022b5a7494d4358f3a64fff236cf7/rust/lance/src/index/optimize.rs). This prototype changes one reader function. It adds no index files, configuration or writer work.

## Measurements

The native HTTP reader served 100 warm queries per subject at 3,000,000 × 128 dimensions, cosine, float16, 6 GiB reader budget and default probes. All arms used the same synthetic corpus, query seed and exact reference over stored float16 values. Latencies are p50 / p95 in milliseconds. Each recall cell lists all / 10% / 1%.

| Reader and layout | Segments × partitions | Maintenance | Recall@10 | All, ms | 10%, ms | 1%, ms |
| --- | --- | --- | --- | --- | --- | --- |
| Baseline, as loaded | 60 × 91 | Before maintenance | .822 / .998 / 1.000 | 85.7 / 123.8 | 69.3 / 80.3 | 87.8 / 103.1 |
| Baseline, tiers | 15 × 91 | Retained tier publication | .823 / .998 / 1.000 | 45.5 / 209.4 | 62.0 / 151.9 | 74.2 / 90.4 |
| Prototype, same tiers | 15 × 91 | Same publication | .996 / 1.000 / 1.000 | 56.5 / 124.3 | 64.0 / 76.7 | 71.7 / 89.4 |
| Prototype, fresh maintenance | 15 × 91 | 60 → 15, four runs | .996 / 1.000 / 1.000 | 60.6 / 121.5 | 51.2 / 102.4 | 78.8 / 96.4 |

On the matched tier publication, telemetry recorded 300 / 300 / 300 candidates for the control and 1,200 / 547 / 300 for the prototype. The earlier `m3_rb` report recorded 300 for the unrestricted subject. Reader startup loads the namespace's published retrieval policy, so a TOML change on a reused namespace did not establish a deeper rescore. There were zero query errors. The unrestricted subject's worst recall improved from 0.5 to 0.9. The [earlier single-segment result](million-row-query.md#latency-warm) was 0.972 recall and 18 ms p50 at 8 GiB, after a 2,147 s maintenance run on another load.

| Maintenance invocation | Duration, s | Fragments after | Vector segments after | Vector merges | Largest logged index merge, s |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 40.0 | 60 | 15 | 15 | 11.9 |
| 2 | 174.1 | 30 | 15 | 15 | 85.1 |
| 3 | 164.1 | 15 | 15 | 0 | 81.5 |
| 4 | 24.0 | 15 | 15 | 0 | — |

Each vector merge reported 200,000 rows and 0.1 s to the telemetry's precision. All four invocations finished within 300 s each, including compaction and publication. Total was 402.2 s. The last run performed zero merges and left the fragment and segment counts unchanged. All 15 final segments shared one centroid tensor.

The archived load's 60 writes of 50,000 rows took 54.6 / 65.3 s p50 / p95 (nearest rank). On the prototype, one additional 50,000-row HTTP write took 67.0 s. It produced segment 16 at 3.05 million rows, still with the same 91 centroids. One append gives no latency distribution.

**Cold-read cost.** On the matched tiers, the unrestricted subject's first dense query used 3,116 storage requests and 568,443,821 bytes, up from 1,017 requests and 530,778,905 bytes. A broader rescore reads more scattered stored vectors. The timings cover local storage with a fresh reader mirror and a shared host. Remote request latency, concurrent traffic and other vector distributions remain unmeasured.

<details>
<summary>Exact reproduction commands</summary>


Build the prototype at `28481045b3bbd1461a0ac97c51957dd8abae3e85` with `PATH=$HOME/.cargo/bin:$PATH fyrr-cargo build --release`. Set `NEW` to its absolute binary path. The control reader's source matches `6a701068`. The archived control binary had SHA-256 `87a732d2725feec98fd391441b7c8c75af356ed840a9b4a786df12758bdfbab5`. Set `OLD` to the control binary, `LOCK` to the shared benchmark lock, `OUT` to a private directory, and `SRC` to the parent of the two read-only benchmark namespaces. Run from the prototype checkout.

```sh
set -eu
for pair in 'scale-16vq3wq3 clean' 'scale-copy2-tier retained'; do
    set -- $pair
    df -h / "$OUT"
    python3 -c 'import shutil,sys; assert all(shutil.disk_usage(p).free >= 30*2**30 for p in sys.argv[1:])' / "$OUT"
    cp -a --reflink=auto "$SRC/$1" "$OUT/$2"
done

measure() {
    flock "$LOCK" scripts/bench-scale --binary "$1" --reuse "$OUT/$2" \
        --memory 6442450944 --queries 100 --native-recall 0 > "$OUT/$3.json" 2> "$OUT/$3.err"
}
measure "$OLD" clean control-60
```

The archive called `scale-copy2-tier` had already replaced all 15 tier outputs. Its head at Lance version 259 had 15 different centroid tensors and 274–295 partitions per segment. Restore the retained publication at version 244 on the private copy. This supplies the same 15 physical segments to both readers. The following commands use the benchmark's native writer launcher, which limits index builds to cores 0–3 and two writer threads.

```sh
flock "$LOCK" python3 - "$OUT/retained" "$OLD" <<'PY'
import json, runpy, sys
from argparse import Namespace
from pathlib import Path
bench = runpy.run_path("scripts/bench-scale")
work = Path(sys.argv[1]).resolve()
args = Namespace(binary=sys.argv[2], memory=[6442450944], maintain_step=None)
run = bench["Run"](args, work, json.loads((work / "secrets.json").read_text()))
pubs = work / "storage/namespaces/bench-scale/publications"
target, = [p.stem for p in pubs.glob("*.json") if json.loads(p.read_text())["lance_version"] == 244]
assert run.run_measured(["writer", "restore", target])[2] == 0
PY
measure "$OLD" retained control-tier
measure "$NEW" retained wide-tier

flock "$LOCK" python3 - "$OUT/clean" "$NEW" > "$OUT/maintain.jsonl" <<'PY'
import json, runpy, sys
from argparse import Namespace
from pathlib import Path
bench = runpy.run_path("scripts/bench-scale")
work = Path(sys.argv[1]).resolve()
args = Namespace(binary=sys.argv[2], memory=[6442450944], maintain_step=None)
run = bench["Run"](args, work, json.loads((work / "secrets.json").read_text()))
for _ in range(8):
    result = bench["maintain_once"](run, work)
    print(json.dumps(result), flush=True)
    step = result["run"]
    assert step["exit_code"] == 0 and not any("skipped" in line for line in step["merge_steps"])
    if not step["merge_steps"] and result["before"] == result["after"]:
        break
else:
    raise RuntimeError("maintenance did not converge")
PY
flock "$LOCK" python3 - "$OUT/retained" "$NEW" > "$OUT/append.json" <<'PYAPPEND'
import json, runpy, sys, time
from argparse import Namespace
from pathlib import Path
bench = runpy.run_path("scripts/bench-scale")
work = Path(sys.argv[1]).resolve()
args = Namespace(binary=sys.argv[2], memory=[6442450944], maintain_step=None)
run = bench["Run"](args, work, json.loads((work / "secrets.json").read_text()))
corpus = bench["Corpus"](3050000,128)
body = corpus.body(3000000)
before = run.status()
writer = run.spawn(["writer","serve","--jobs","1"], args.memory[0], pin=True)
try:
    api = bench["Server"](run.writer_address)
    for _ in range(600):
        try:
            status, reply, elapsed = api.call("POST", "/v1/namespaces/bench-scale/documents",
                run.secrets["env"]["FYRR_BENCH_WRITE_KEY"], body)
            break
        except ConnectionRefusedError:
            time.sleep(.1)
    else:
        sys.exit("writer unavailable")
    assert status == 200 and reply["upserted"] == 50000, (status,reply.get("error"))
finally:
    peak = run.stop(writer)
print(json.dumps(dict(upserted=reply["upserted"],seconds=elapsed,peak_rss_mib=peak,
      segments_before=before["index_segments"]["vector_idx"],segments_after=run.status()["index_segments"]["vector_idx"])))
PYAPPEND
measure "$NEW" clean converged
```

</details>
