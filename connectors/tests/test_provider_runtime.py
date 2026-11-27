"""Provider metadata, the bundled providers, fyrr's runtime boundary and the
provider test kit."""

import json
from types import SimpleNamespace

import pytest
import sample_provider

from fyrr_connectors import (
    Connector,
    ContentBatch,
    ContractViolation,
    document_changes,
    provider_info,
    runtime,
    testkit,
)
from fyrr_connectors.validate import MAX_READERS

SENTINEL = "SENTINEL-rt-0b52"
BUNDLED = ["github", "google-drive", "jira", "local-files", "notion", "slack"]
LIMITS = {"max_rows": 100, "max_bytes": 1 << 20, "document_max_bytes": 1 << 16}


def test_provider_info_derives_kinds_and_config_schema() -> None:
    info = sample_provider.info()
    testkit.check_provider_info(info)
    assert info["sync_kinds"] == ["content", "reconcile"]
    assert info["connector"] == "sample_provider:SampleConnector"
    assert info["config_schema"]["settings"]["properties"]["api_token"]["x-fyrr-secret"] is True
    with pytest.raises(TypeError):
        provider_info(name="x", connector=object, min_sdk_version="0.1.0")


def test_bundled_connectors_are_installed_providers() -> None:
    discovered = json.loads(runtime.discover())
    assert discovered["sdk_version"] == "0.1.2"
    names = [entry["entry_point"] for entry in discovered["providers"]]
    assert names == BUNDLED
    for entry in discovered["providers"]:
        assert entry["distribution"] == "fyrr-connectors"
        assert "error" not in entry
        testkit.check_provider(entry["entry_point"])
    fixed_only = {
        entry["entry_point"]
        for entry in discovered["providers"]
        if entry["info"]["connection_types"] == ["fixed"]
    }
    assert fixed_only == {"local-files", "notion"}


def test_test_kit_runs_and_checks_a_sync() -> None:
    result = testkit.run_sync(
        sample_provider.info(),
        "content",
        settings={"api_token": {"env": "SAMPLE_TOKEN"}},
        scope={"titles": ["one", "two", "three"]},
    )
    assert [len(batch) for batch in result.record_batches] == [2, 1]
    assert result.cursor == "4"
    assert [document["cursor"] for document in result.documents] == [
        {"start": None, "end": "2"},
        {"start": "2", "end": "4"},
    ]
    assert (
        result.documents[0]["body"]["changes"][0]["metadata"]["created_at"]
        == "2026-09-30T09:41:12.512Z"
    )
    listing = testkit.run_sync(
        sample_provider.info(),
        "reconcile",
        settings={"api_token": {"env": "SAMPLE_TOKEN"}},
        scope={"titles": ["one"]},
    )
    assert listing.documents[0]["body"]["root_ids"] == ["doc-0"]


def test_test_kit_refuses_bad_config_and_bad_batches() -> None:
    with pytest.raises(AssertionError, match="settings does not match"):
        testkit.run_sync(
            sample_provider.info(),
            "content",
            settings={"api_token": SENTINEL},
            scope={"titles": ["x"]},
        )

    class WrongKind(sample_provider.SampleConnector):
        def content(self, cursor):
            yield "not a batch"

    info = dict(sample_provider.info(), connector="test_provider_runtime:WrongKindHolder.Connector")
    WrongKindHolder.Connector = WrongKind
    with pytest.raises(ContractViolation, match="yields ContentBatch values"):
        testkit.run_sync(
            info, "content", settings={"api_token": {"env": "T"}}, scope={"titles": ["x"]}
        )


class WrongKindHolder:
    Connector: type = Connector


class Failing(Connector):
    def content(self, cursor):
        raise ValueError(f"document text {SENTINEL}")


class SelfViolating(Connector):
    def content(self, cursor):
        raise ContractViolation(f"my own message {SENTINEL}")


def _run(connector: type[Connector]) -> list:
    info = provider_info(name="x", connector=connector, min_sdk_version="0.1.0")
    source = {
        "source_id": "s",
        "provider": "x",
        "permissions": "sync",
        "settings": {},
        "scope": {},
        "limits": LIMITS,
    }
    original = runtime.provider_entry_point

    class Entry:
        def load(self):
            return lambda: info

    runtime.provider_entry_point = lambda name: Entry()
    try:
        return list(runtime.run(json.dumps(source), "content", None))
    finally:
        runtime.provider_entry_point = original


def test_runtime_reports_provider_failures_without_messages() -> None:
    with pytest.raises(runtime.ProviderFailure) as caught:
        _run(Failing)
    described = json.loads(caught.value.args[0])
    assert described["type"] == "builtins.ValueError"
    assert any("in content" in frame for frame in described["frames"])
    assert "message" not in described
    assert SENTINEL not in caught.value.args[0]
    with pytest.raises(runtime.ProviderFailure) as caught:
        _run(SelfViolating)
    assert SENTINEL not in caught.value.args[0], "a provider-raised violation keeps no message"


def test_runtime_keeps_sdk_violation_messages() -> None:
    try:
        document_changes([object()])
    except ContractViolation as error:
        described = runtime.describe(error)
    assert described["message"] == "document_changes takes Upsert and Delete values"


def test_testkit_bounds_full_arrow_buffers_before_materializing() -> None:
    import pyarrow as pa

    from fyrr_connectors.connector import Limits

    class Extension(Connector):
        def content(self, cursor):
            rows = document_changes([])
            rows = rows.append_column(
                pa.field("extension", pa.binary()), pa.array([], type=pa.binary())
            )
            # A zero-row slice still retains its large backing buffer.
            storage = pa.array([b"x" * (4 << 20)], type=pa.binary()).slice(0, 0)
            rows = rows.set_column(rows.num_columns - 1, rows.schema.field("extension"), storage)
            yield ContentBatch(changes=rows, cursor="done")

    WrongKindHolder.Connector = Extension
    info = provider_info(name="extension", connector=Extension, min_sdk_version="0.1.0")
    info["connector"] = "test_provider_runtime:WrongKindHolder.Connector"
    with pytest.raises(AssertionError, match="exceeds max_bytes"):
        testkit.run_sync(
            info,
            "content",
            limits=Limits(max_rows=10, max_bytes=2 << 20, document_max_bytes=1 << 20),
        )


def test_testkit_document_limit_matches_rust_default() -> None:
    assert testkit.DEFAULT_LIMITS.document_max_bytes == 8 << 20


def test_bundled_metadata_does_not_import_connector_dependencies():
    import subprocess
    import sys

    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json, sys
from fyrr_connectors import runtime
result = json.loads(runtime.discover())
assert all('info' in provider for provider in result['providers'])
assert not any(name.endswith('.connector') and name.startswith('fyrr_connectors.')
               for name in sys.modules if name != 'fyrr_connectors.connector')
assert not any(name in sys.modules for name in ['requests', 'jwt', 'google.auth', 'openpyxl'])
""",
        ],
        capture_output=True,
        text=True,
    )
    assert probe.returncode == 0, probe.stderr


@pytest.mark.parametrize("provider", json.loads(runtime.discover())["providers"], ids=BUNDLED)
def test_bundled_lazy_metadata_matches_the_real_connector_contract(provider):
    from fyrr_connectors.runtime import connector_class

    info = provider["info"]
    real = provider_info(
        name=info["name"],
        connector=connector_class(info),
        min_sdk_version=info["min_sdk_version"],
        connection_types=info["connection_types"],
        description=info["description"],
    )
    assert info == real


def test_local_files_grants_every_configured_reader(tmp_path) -> None:
    (tmp_path / "notes.md").write_text("shared notes")
    readers = ("grp:engineering", "usr:ada@example.com")
    result = testkit.run_sync(
        "local-files", "content", scope={"paths": [str(tmp_path)]}, readers=readers
    )
    [change] = result.documents[0]["body"]["changes"]
    assert change["root_id"] == "notes.md"
    assert sorted(change["readers"]) == sorted(readers)
    # The revision covers the reader set: another set republishes the file.
    other = testkit.run_sync(
        "local-files", "content", scope={"paths": [str(tmp_path)]}, readers=readers[:1]
    )
    assert (
        other.documents[0]["body"]["changes"][0]["source_revision"] != (change["source_revision"])
    )


def test_local_files_normalizes_controls_without_changing_raw_revision_identity(tmp_path) -> None:
    import hashlib

    readers = ("grp:engineering",)
    raw = b"alpha\0beta\x01gamma\tline\nnext\rline\x1fend"
    path = tmp_path / "notes.txt"
    path.write_bytes(raw)
    result = testkit.run_sync(
        "local-files", "content", scope={"paths": [str(tmp_path)]}, readers=readers
    )
    [change] = result.documents[0]["body"]["changes"]
    assert change["sections"][0]["text"] == "alpha beta gamma\tline\nnext\rline end"
    fingerprint = hashlib.sha256()
    for token in sorted(readers):
        encoded = token.encode("utf-8")
        fingerprint.update(len(encoded).to_bytes(8, "big"))
        fingerprint.update(encoded)
    fingerprint.update(raw)
    assert change["source_revision"] == fingerprint.hexdigest()
    # The same extracted text from different source bytes remains a new revision.
    path.write_bytes(raw.replace(b"\0", b" "))
    other = testkit.run_sync(
        "local-files", "content", scope={"paths": [str(tmp_path)]}, readers=readers
    ).documents[0]["body"]["changes"][0]
    assert other["sections"] == change["sections"]
    assert other["source_revision"] != change["source_revision"]
    # Invalid input still takes the explicit quarantine path, never normalization.
    path.write_bytes(b"invalid\xff")
    invalid = testkit.run_sync(
        "local-files", "content", scope={"paths": [str(tmp_path)]}, readers=readers
    ).documents[0]["body"]["changes"][0]
    assert invalid["sections"][0]["text"] == "\0"


@pytest.mark.parametrize(
    ("count", "parts"), [(0, [0]), (1, [1]), (3, [3]), (4, [3, 1]), (6, [3, 3]), (7, [3, 3, 1])]
)
def test_listing_batches_cut_roots_into_parts_of_one_listing(count, parts) -> None:
    context = SimpleNamespace(limits=SimpleNamespace(max_rows=3))
    roots = [f"r{index}" for index in range(count)]
    batches = list(Connector(context).listing_batches(iter(roots), lambda: True))
    assert [len(batch.root_ids) for batch in batches] == parts
    assert [id for batch in batches for id in batch.root_ids] == roots
    assert [batch.part for batch in batches] == list(range(len(parts)))
    assert [batch.last for batch in batches] == [False] * (len(parts) - 1) + [True]
    # Only the final part attests a complete listing.
    assert [batch.complete for batch in batches] == [False] * (len(parts) - 1) + [True]
    assert len({batch.listing_id for batch in batches}) == 1
    assert [batch.cursor for batch in batches] == [
        f"{batches[0].listing_id}:{part}" for part in range(len(parts))
    ]


def test_a_listing_longer_than_the_contract_allows_in_one_part_is_split(tmp_path) -> None:
    for index in range(MAX_READERS + 1):
        (tmp_path / f"f{index}.txt").write_text("x")
    result = testkit.run_sync(
        "local-files", "reconcile", scope={"paths": [str(tmp_path)]}, readers=("grp:a",)
    )
    parts = [document["body"]["root_ids"] for document in result.documents]
    assert len(parts) > 1 and max(map(len, parts)) <= MAX_READERS
    assert sum(map(len, parts)) == MAX_READERS + 1
