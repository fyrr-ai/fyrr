"""SDK validation follows the published contract and worker boundaries."""

import math
import re
import subprocess
import sys
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
import sample_provider

from fyrr_connectors import runtime, testkit, validate
from fyrr_connectors.config import SECRET_MARK


def test_semantic_limits_match_the_worker():
    rust = (Path(__file__).parents[2] / "crates/fyrr-core/src/limits.rs").read_text()
    constants = re.findall(
        r"pub const (MAX_\w+): usize = ([\d_* ]+);", rust.split("pub mod mcp")[0]
    )
    assert constants
    expected = {
        name: math.prod(int(part.replace("_", "")) for part in value.split("*"))
        for name, value in constants
    }
    actual = {name: value for name, value in vars(validate).items() if name.startswith("MAX_")}
    assert actual == expected


def _secret_schema():
    return {
        "type": "object",
        "$defs": {
            "secret/key~": {
                SECRET_MARK: True,
                "type": "object",
                "properties": {"env": {"type": "string"}},
                "required": ["env"],
                "additionalProperties": False,
            },
            "account": {
                "type": "object",
                "properties": {"token": {"$ref": "#/$defs/secret~1key~0"}},
            },
        },
        "properties": {
            "accounts": {"type": "array", "items": {"$ref": "#/$defs/account"}},
        },
    }


def test_secret_checks_follow_local_references_and_array_items():
    schema = _secret_schema()
    found = testkit._secret_properties(schema)
    assert [path for path, _ in found] == ["accounts[].token"]
    info = sample_provider.info()
    info["config_schema"]["settings"] = schema
    testkit.check_provider_info(info)
    schema["$defs"]["secret/key~"] = {SECRET_MARK: True, "type": "string"}
    with pytest.raises(AssertionError, match=r"accounts.*token.*must accept"):
        testkit.check_provider_info(info)


@pytest.mark.parametrize(
    "reference", ["https://invalid.example/schema", "#missing", "#/$defs/missing", "#"]
)
def test_secret_reference_traversal_fails_closed(reference):
    schema = _secret_schema()
    schema["properties"]["accounts"] = {"$ref": reference}
    with pytest.raises(AssertionError, match="secret schema"):
        testkit._secret_properties(schema)


def test_secret_checks_keep_the_root_resolver_for_annotated_references():
    schema = _secret_schema()
    schema["properties"] = {
        "token": {SECRET_MARK: True, "$ref": "#/$defs/secret~1key~0"},
    }
    info = sample_provider.info()
    info["config_schema"]["settings"] = schema
    testkit.check_provider_info(info)


@pytest.mark.parametrize("count", [0, 1, 2])
def test_runtime_and_testkit_share_entry_point_lookup(monkeypatch, count):
    entries = [EntryPoint("sample", "sample_provider:info", "fyrr.connectors")] * count
    monkeypatch.setattr(runtime.metadata, "entry_points", lambda **_: entries)
    if count == 1:
        assert testkit.provider_entry_point("sample") is runtime.provider_entry_point("sample")
    else:
        for lookup in (runtime.provider_entry_point, testkit.provider_entry_point):
            with pytest.raises(LookupError, match="exactly one provider"):
                lookup("sample")


def test_testkit_validation_stays_active_with_optimized_python(tmp_path):
    probe = subprocess.run(
        [
            sys.executable,
            "-O",
            "-c",
            """
import sys
from pathlib import Path
from fyrr_connectors import testkit
checks = [
    lambda: testkit.check_provider_info({}),
    lambda: testkit.check_config({'config_schema': {
        'settings': {'type': 'object', 'required': ['token']}, 'scope': {'type': 'object'}
    }}),
    lambda: testkit.assert_golden(testkit.SyncResult(kind='content'), Path(sys.argv[1])),
]
for check in checks:
    try:
        check()
    except AssertionError:
        continue
    raise SystemExit('validation was disabled')
""",
            str(tmp_path / "missing.json"),
        ],
        capture_output=True,
        text=True,
    )
    assert probe.returncode == 0, probe.stderr


@pytest.mark.parametrize(
    ("minimum", "refusal"),
    [
        ("0.1.0", None),
        ("0.1.3", None),
        ("0.1.4", "newer than the installed SDK"),
        ("0.2.0", "must be in SDK series 0.1"),
        ("1.0.0", "must be in SDK series 0.1"),
    ],
    ids=["oldest", "installed", "newer", "next-series", "stable-series"],
)
def test_testkit_requires_a_minimum_in_the_installed_series(monkeypatch, minimum, refusal):
    monkeypatch.setattr(testkit.runtime, "sdk_version", lambda: "0.1.3")
    info = sample_provider.info()
    info["min_sdk_version"] = minimum
    if refusal is None:
        testkit.check_provider_info(info)
    else:
        with pytest.raises(AssertionError, match=refusal):
            testkit.check_provider_info(info)
