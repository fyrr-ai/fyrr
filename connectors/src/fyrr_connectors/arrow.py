"""Contract-exact Arrow document batches built from Upsert and Delete values.

The bundled JSON schema fixes types, nested nullability and metadata. The
provider host sends these batches to fyrr's worker as Arrow IPC streams, and
the worker checks them.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.resources import files
from typing import Any

import pyarrow as pa

from .validate import ContractViolation, check_document_changes_json

CONTRACT_KEY = "fyrr.contract"
VERSION_KEY = "fyrr.schema_version"
DOCUMENT_CHANGES_CONTRACT = "connector.document_changes"


def published_document_changes_schema() -> pa.Schema:
    """Load the bundled Arrow contract, including nested fields and metadata."""
    published = json.loads(
        (files("fyrr_connectors") / "schemas/connector-document-changes.arrow.json").read_text()
    )
    return pa.schema(
        [_arrow_field(field) for field in published["fields"]],
        metadata=published.get("metadata") or None,
    )


def _arrow_field(field: Mapping[str, Any]) -> pa.Field:
    return pa.field(field["name"], _arrow_type(field["data_type"]), nullable=field["nullable"])


def _arrow_type(data_type: Any) -> pa.DataType:
    simple = {
        "Utf8": pa.string(),
        "Int64": pa.int64(),
        "Float32": pa.float32(),
        "UInt32": pa.uint32(),
        "Boolean": pa.bool_(),
    }
    if isinstance(data_type, str):
        return simple[data_type]
    ((kind, inner),) = data_type.items()
    if kind == "List":
        return pa.list_(_arrow_field(inner))
    if kind == "Struct":
        return pa.struct([_arrow_field(child) for child in inner])
    if kind == "Timestamp":
        unit, zone = inner
        return pa.timestamp({"Microsecond": "us"}[unit], tz=zone)
    raise ValueError(f"unsupported Arrow type {kind} in the published schema")


DOCUMENT_CHANGES_SCHEMA = published_document_changes_schema()
SECTION_TYPE = DOCUMENT_CHANGES_SCHEMA.field("sections").type.value_type

_METADATA_FIELDS = ("title", "author", "team", "project", "status")


@dataclass(frozen=True, kw_only=True)
class Location:
    """Where a section sits: `heading`, `sheet`, `slide`, `page`, `message`, …"""

    kind: str
    label: str | None = None
    index: int | None = None


@dataclass(frozen=True, kw_only=True)
class Section:
    """Document text in order; `readers` narrows a restricted part. `vector`
    is the section's own embedding: it is stored in place of one computed from
    the text, so the text must be one chunk (at most 16 KiB) and the vector has
    the namespace's dimension, finite elements and a nonzero length."""

    text: str
    location: Location | None = None
    readers: Sequence[str] | None = None
    vector: Sequence[float] | None = None


@dataclass(frozen=True, kw_only=True)
class Metadata:
    """Document metadata. Unknown values stay None, never the ingest time."""

    source_uri: str
    source_type: str
    title: str | None = None
    author: str | None = None
    team: str | None = None
    project: str | None = None
    status: str | None = None
    labels: Sequence[str] = ()
    created_at: datetime | None = None
    updated_at: datetime | None = None
    connector_map: Mapping[str, Any] | None = None


@dataclass(frozen=True, kw_only=True)
class Upsert:
    """A complete document: it replaces every record of `root_id`."""

    root_id: str
    source_revision: str
    metadata: Metadata
    sections: Sequence[Section]
    readers: Sequence[str] = ()
    containers: Sequence[str] = ()
    attributes: Sequence[str] = ()
    revision_order: int | None = None
    expected_prior_revision: str | None = None
    permission_revision: str | None = None


@dataclass(frozen=True, kw_only=True)
class Delete:
    """Removes every record of `root_id`."""

    root_id: str
    source_revision: str | None = None
    revision_order: int | None = None
    expected_prior_revision: str | None = None
    permission_revision: str | None = None


def _instant(field: str, value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ContractViolation(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _row(change: Upsert | Delete) -> dict[str, Any]:
    if not isinstance(change, Upsert | Delete):
        raise ContractViolation("document_changes takes Upsert and Delete values")
    row: dict[str, Any] = {
        "root_id": change.root_id,
        "source_revision": change.source_revision,
        "revision_order": change.revision_order,
        "expected_prior_revision": change.expected_prior_revision,
        "permission_revision": change.permission_revision,
        "labels": [],
        "attributes": [],
        "sections": [],
        "readers": [],
        "containers": [],
    }
    if isinstance(change, Delete):
        row["op"] = "delete"
        return row
    metadata = change.metadata
    row.update(
        op="upsert",
        readers=list(change.readers),
        containers=list(change.containers),
        attributes=list(change.attributes),
        source_uri=metadata.source_uri,
        source_type=metadata.source_type,
        labels=list(metadata.labels),
        created_at=_instant("metadata.created_at", metadata.created_at),
        updated_at=_instant("metadata.updated_at", metadata.updated_at),
        connector_map=(
            None
            if metadata.connector_map is None
            else json.dumps(metadata.connector_map, separators=(",", ":"), ensure_ascii=False)
        ),
        sections=[
            {
                "text": section.text,
                "location_kind": section.location.kind if section.location else None,
                "location_label": section.location.label if section.location else None,
                "location_index": section.location.index if section.location else None,
                "readers": None if section.readers is None else list(section.readers),
                "vector": None if section.vector is None else list(section.vector),
            }
            for section in change.sections
        ],
    )
    for field in _METADATA_FIELDS:
        row[field] = getattr(metadata, field)
    return row


def document_changes(changes: Iterable[Upsert | Delete]) -> pa.RecordBatch:
    """Build one contract-exact document-change batch and check it."""
    batch = pa.RecordBatch.from_pylist(
        [_row(change) for change in changes], schema=DOCUMENT_CHANGES_SCHEMA
    )
    check_document_changes(batch)
    return batch


def check_document_changes(batch: pa.RecordBatch) -> None:
    """Check a batch against this SDK's contract: exact schema, no nulls in
    non-nullable fields at any depth, and every row's semantic rules."""
    if not isinstance(batch, pa.RecordBatch):
        raise ContractViolation("content batches are pyarrow.RecordBatch values")
    differences = schema_differences(DOCUMENT_CHANGES_SCHEMA, batch.schema)
    if differences:
        raise ContractViolation("Arrow schema differs from the contract: " + "; ".join(differences))
    for field, column in zip(batch.schema, batch.columns, strict=True):
        _check_nulls(column, field, field.name)
    check_document_changes_json(rows_to_json(batch))


def schema_differences(expected: pa.Schema, found: pa.Schema) -> list[str]:
    """Field-by-field differences (names, types, nullability) and metadata."""
    problems = _field_differences(list(expected), list(found), "")
    wanted = expected.metadata or {}
    present = found.metadata or {}
    for key, value in wanted.items():
        if present.get(key) != value:
            problems.append(f"schema metadata {key.decode()} is missing or different")
    return problems


def _field_differences(expected: list[pa.Field], found: list[pa.Field], prefix: str) -> list[str]:
    problems = []
    present = {field.name: field for field in found}
    names = [field.name for field in expected]
    for field in expected:
        path = f"{prefix}{field.name}"
        other = present.get(field.name)
        if other is None:
            problems.append(f"missing field {path}")
            continue
        problems.extend(_type_differences(field, other, path))
    # Like the Rust check, field order and list item names do not matter;
    # unlike it, this SDK writes exactly its own minor, so no extra fields.
    for field in found:
        if field.name not in names:
            problems.append(f"unexpected field {prefix}{field.name}")
    return problems


def _type_differences(expected: pa.Field, found: pa.Field, path: str) -> list[str]:
    problems = []
    if expected.nullable != found.nullable:
        problems.append(f"field {path} must have nullable={expected.nullable}")
    want, have = expected.type, found.type
    if pa.types.is_struct(want) and pa.types.is_struct(have):
        problems.extend(
            _field_differences(
                [want.field(i) for i in range(want.num_fields)],
                [have.field(i) for i in range(have.num_fields)],
                f"{path}.",
            )
        )
    elif pa.types.is_list(want) and pa.types.is_list(have):
        problems.extend(_type_differences(want.value_field, have.value_field, f"{path}[]"))
    elif want != have:
        problems.append(f"field {path} has type {have}, expected {want}")
    return problems


def _check_nulls(array: pa.Array, field: pa.Field, path: str) -> None:
    if not field.nullable and array.null_count:
        raise ContractViolation(f"field {path} is not nullable but holds nulls")
    if pa.types.is_list(field.type):
        _check_nulls(array.flatten(), field.type.value_field, f"{path}[]")
    elif pa.types.is_struct(field.type):
        for index in range(field.type.num_fields):
            child = field.type.field(index)
            _check_nulls(array.field(index), child, f"{path}.{child.name}")


def rfc3339(value: datetime) -> str:
    """The contract's instant format (as the Rust side writes it): UTC with
    `Z`, milli- or microseconds only when nonzero."""
    value = value.astimezone(UTC)
    text = value.strftime("%Y-%m-%dT%H:%M:%S")
    if value.microsecond:
        if value.microsecond % 1000 == 0:
            text += f".{value.microsecond // 1000:03d}"
        else:
            text += f".{value.microsecond:06d}"
    return text + "Z"


def rows_to_json(batch: pa.RecordBatch) -> list[dict[str, Any]]:
    """The contract (JSON) form of every row, as in the golden fixtures."""
    changes = []
    for row in batch.to_pylist():
        change: dict[str, Any] = {"op": row["op"], "root_id": row["root_id"]}
        for field in (
            "source_revision",
            "revision_order",
            "expected_prior_revision",
            "permission_revision",
        ):
            if row.get(field) is not None:
                change[field] = row[field]
        change["readers"] = row["readers"]
        change["containers"] = row["containers"]
        if row.get("source_uri") is not None or row.get("source_type") is not None:
            metadata: dict[str, Any] = {
                "source_uri": row.get("source_uri"),
                "source_type": row.get("source_type"),
            }
            for field in _METADATA_FIELDS:
                if row.get(field) is not None:
                    metadata[field] = row[field]
            metadata["labels"] = row["labels"]
            for field in ("created_at", "updated_at"):
                if row.get(field) is not None:
                    metadata[field] = rfc3339(row[field])
            if row.get("connector_map") is not None:
                try:
                    metadata["connector_map"] = json.loads(row["connector_map"])
                except ValueError:
                    raise ContractViolation("connector_map is not JSON") from None
            change["metadata"] = metadata
        change["attributes"] = row["attributes"]
        sections = []
        for section in row["sections"]:
            entry: dict[str, Any] = {"text": section["text"]}
            if section.get("location_kind") is not None:
                location: dict[str, Any] = {"kind": section["location_kind"]}
                if section.get("location_label") is not None:
                    location["label"] = section["location_label"]
                if section.get("location_index") is not None:
                    location["index"] = section["location_index"]
                entry["location"] = location
            if section.get("readers") is not None:
                entry["readers"] = section["readers"]
            if section.get("vector") is not None:
                entry["vector"] = section["vector"]
            sections.append(entry)
        change["sections"] = sections
        changes.append(change)
    return changes
