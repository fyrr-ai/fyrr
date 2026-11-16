"""The hosted S3 shards must partition the existing case owners exactly."""

import json
import runpy
from collections import Counter
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PLAN = runpy.run_path(str(ROOT / "scripts/ci-cases"))
MANIFEST = json.loads((ROOT / "scripts/ci-cases.json").read_text())


@pytest.mark.parametrize("count", [1, 4, 6])
def test_shards_execute_every_s3_owner_once(count):
    expected = {row for row in MANIFEST if row.startswith("s3-")}
    selected = [
        row
        for index in range(1, count + 1)
        for row in PLAN["s3_rows"](MANIFEST, shard=f"{index}/{count}")
    ]
    assert Counter(selected) == Counter(dict.fromkeys(expected, 1))
    # Each row retains its original exact cases, including its child campaigns.
    owners = [case for row in selected for case in MANIFEST[row]["cases"]]
    assert len(owners) == len(set(owners))


def test_quick_gate_is_a_nonempty_subset_of_the_full_suite():
    smoke = set(PLAN["s3_rows"](MANIFEST, quick=True))
    assert smoke < set(PLAN["s3_rows"](MANIFEST))
    assert {"s3-native-lance-roundtrip", "s3-block4", "s3-namespace-contracts"} <= smoke


@pytest.mark.parametrize("shard", ["0/6", "7/6", "1/0"])
def test_invalid_shards_are_rejected(shard):
    with pytest.raises(ValueError, match="shard must"):
        PLAN["s3_rows"](MANIFEST, shard=shard)
