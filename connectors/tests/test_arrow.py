"""Arrow document-change batches match the published contract exactly."""

import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pytest

from fyrr_connectors import (
    DOCUMENT_CHANGES_SCHEMA,
    ContractViolation,
    Delete,
    Location,
    Metadata,
    Section,
    Upsert,
    check_document_changes,
    document_changes,
)
from fyrr_connectors.arrow import rows_to_json
from fyrr_connectors.testkit import published_document_changes_schema

REPO = Path(__file__).resolve().parents[2]
SENTINEL = "SENTINEL-py-7c3e"


def _from_fixture(change: dict) -> Upsert | Delete:
    common = {
        key: change[key]
        for key in ("source_revision", "revision_order", "expected_prior_revision")
        if key in change
    }
    if "permission_revision" in change:
        common["permission_revision"] = change["permission_revision"]
    if change["op"] == "delete":
        return Delete(root_id=change["root_id"], **common)
    meta = dict(change["metadata"])
    for key in ("created_at", "updated_at"):
        if key in meta:
            meta[key] = datetime.fromisoformat(meta[key])
    sections = [
        Section(
            text=section["text"],
            location=Location(**section["location"]) if "location" in section else None,
            readers=section.get("readers"),
        )
        for section in change["sections"]
    ]
    return Upsert(
        root_id=change["root_id"],
        readers=change["readers"],
        containers=change["containers"],
        attributes=change["attributes"],
        metadata=Metadata(**meta),
        sections=sections,
        **common,
    )


def test_schema_equals_the_published_contract() -> None:
    published = published_document_changes_schema()
    assert DOCUMENT_CHANGES_SCHEMA.equals(published, check_metadata=True)
    # pyarrow compares list item nullability and struct children too.
    assert DOCUMENT_CHANGES_SCHEMA.field("sections").type.value_field.nullable is False
    assert DOCUMENT_CHANGES_SCHEMA.field("readers").type.value_field.nullable is False


def test_golden_fixture_round_trips_through_arrow() -> None:
    fixture = json.loads(
        (REPO / "fixtures/contracts/connector-batch/document-changes.json").read_text()
    )
    changes = fixture["body"]["changes"]
    batch = document_changes(_from_fixture(change) for change in changes)
    assert batch.schema.equals(DOCUMENT_CHANGES_SCHEMA, check_metadata=True)
    assert rows_to_json(batch) == changes


def test_pyarrow_defaults_are_refused_with_the_differences() -> None:
    rows = [{"op": "delete", "root_id": "a", "readers": [], "sections": []}]
    with pytest.raises(ContractViolation) as caught:
        check_document_changes(pa.RecordBatch.from_pylist(rows))
    message = str(caught.value)
    assert "field op must have nullable=False" in message
    assert "missing field labels" in message
    assert "schema metadata fyrr.contract" in message
    large = DOCUMENT_CHANGES_SCHEMA.set(1, pa.field("root_id", pa.large_string(), nullable=False))
    with pytest.raises(ContractViolation, match="root_id has type large_string"):
        check_document_changes(pa.RecordBatch.from_pylist([], schema=large))


def test_nulls_in_non_nullable_fields_are_refused_at_any_depth() -> None:
    base = document_changes(
        [
            Upsert(
                root_id="a",
                source_revision="1",
                metadata=Metadata(source_uri="x://a", source_type="t"),
                sections=[Section(text="t")],
            )
        ]
    ).to_pylist()
    for mutate, field in [
        (lambda row: row.update(readers=None), "readers"),
        (lambda row: row.update(labels=[None]), "labels[]"),
        (lambda row: row["sections"][0].update(text=None), "sections[].text"),
    ]:
        rows = json.loads(json.dumps(base))
        mutate(rows[0])
        batch = pa.RecordBatch.from_pylist(rows, schema=DOCUMENT_CHANGES_SCHEMA)
        with pytest.raises(
            ContractViolation,
            match=rf"field {field.replace('[', '\\[').replace(']', '\\]')} is not nullable",
        ):
            check_document_changes(batch)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda: Upsert(
                root_id="a",
                source_revision="1",
                metadata=Metadata(source_uri="u", source_type="t"),
                sections=[],
            ),
            "at least one section",
        ),
        (
            lambda: Upsert(
                root_id="a",
                source_revision="1",
                readers=[f"bad:{SENTINEL}"],
                metadata=Metadata(source_uri="u", source_type="t"),
                sections=[Section(text="x")],
            ),
            "usr:<id> or grp:<id>",
        ),
        (
            lambda: Upsert(
                root_id="a",
                source_revision="1",
                attributes=[SENTINEL],
                metadata=Metadata(source_uri="u", source_type="t"),
                sections=[Section(text="x")],
            ),
            "key=value",
        ),
        (
            lambda: Upsert(
                root_id=f" {SENTINEL}",
                source_revision="1",
                metadata=Metadata(source_uri="u", source_type="t"),
                sections=[Section(text="x")],
            ),
            "surrounding whitespace",
        ),
        (
            lambda: Upsert(
                root_id="a",
                source_revision="1",
                metadata=Metadata(source_uri="u", source_type="t", title=SENTINEL * 200),
                sections=[Section(text="x")],
            ),
            "metadata.title exceeds",
        ),
        (
            lambda: Upsert(
                root_id="a",
                source_revision="1",
                metadata=Metadata(source_uri="u", source_type="t", created_at=datetime(2026, 1, 1)),
                sections=[Section(text="x")],
            ),
            "timezone-aware",
        ),
    ],
)
def test_semantic_violations_are_refused_without_values(change, message: str) -> None:
    with pytest.raises(ContractViolation) as caught:
        document_changes([change()])
    assert message in str(caught.value)
    assert SENTINEL not in str(caught.value)


def test_a_document_appears_once_per_batch() -> None:
    with pytest.raises(ContractViolation, match="appears twice"):
        document_changes([Delete(root_id="a"), Delete(root_id="a")])
