"""Semantic checks of connector batches in their contract (JSON) form.

These mirror `ConnectorBatch::validate` in `crates/fyrr-core/src/connector.rs`
so provider authors see contract violations in their own tests. The Rust
worker repeats every check at the boundary and is authoritative. Messages name
fields, never values.
"""

from __future__ import annotations

import json
import unicodedata
from collections.abc import Mapping, Sequence
from typing import Any

MAX_ID_BYTES = 512
MAX_URI_BYTES = 4096
MAX_METADATA_VALUE_BYTES = 1024
MAX_LABELS = 32
MAX_CONNECTOR_MAP_BYTES = 16 * 1024
MAX_ATTRIBUTES = 64
MAX_ATTRIBUTE_KEY_BYTES = 128
MAX_ATTRIBUTE_BYTES = 512
MAX_READERS = 16_384
MAX_CURSOR_BYTES = 16 * 1024
MAX_REVISION_BYTES = 256
MAX_READER_REF_BYTES = 600

_ATTRIBUTE_KEY = frozenset("abcdefghijklmnopqrstuvwxyz0123456789._-")


class ContractViolation(ValueError):
    """A batch breaks the connector contract. The message holds no values."""


def check_text(field: str, value: Any, max_bytes: int) -> None:
    if not isinstance(value, str):
        raise ContractViolation(f"{field} is not a string")
    if not value:
        raise ContractViolation(f"{field} is empty")
    if len(value.encode()) > max_bytes:
        raise ContractViolation(f"{field} exceeds {max_bytes} bytes")
    if value.strip() != value:
        raise ContractViolation(f"{field} has surrounding whitespace")
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise ContractViolation(f"{field} contains control characters")


def check_count(field: str, count: int, maximum: int) -> None:
    if count > maximum:
        raise ContractViolation(f"{field} has {count} entries, more than {maximum}")


def check_ids(field: str, values: Sequence[str]) -> None:
    check_count(field, len(values), MAX_READERS)
    for value in values:
        check_text(field, value, MAX_ID_BYTES)


def check_readers(field: str, values: Sequence[str]) -> None:
    """Reader and member references are `usr:<id>` or `grp:<id>`."""
    check_count(field, len(values), MAX_READERS)
    for value in values:
        check_text(field, value, MAX_READER_REF_BYTES)
        prefix, _, rest = value.partition(":")
        if prefix not in ("usr", "grp") or not rest:
            raise ContractViolation(f"{field} entries must be usr:<id> or grp:<id>")


def check_cursor(field: str, value: Any) -> None:
    if not isinstance(value, str) or not value or len(value.encode()) > MAX_CURSOR_BYTES:
        raise ContractViolation(f"{field} must be 1..={MAX_CURSOR_BYTES} bytes")


def check_attributes(values: Sequence[str]) -> None:
    check_count("attributes", len(values), MAX_ATTRIBUTES)
    for value in values:
        check_text("attributes", value, MAX_ATTRIBUTE_BYTES)
        key, separator, rest = value.partition("=")
        if not separator:
            raise ContractViolation("attributes entries must be key=value")
        key_ok = (
            bool(key)
            and len(key.encode()) <= MAX_ATTRIBUTE_KEY_BYTES
            and all(character in _ATTRIBUTE_KEY for character in key)
        )
        if not key_ok or not rest:
            raise ContractViolation("attributes entries must be key=value with a [a-z0-9._-] key")


def check_metadata(metadata: Mapping[str, Any]) -> None:
    check_text("metadata.source_uri", metadata.get("source_uri"), MAX_URI_BYTES)
    check_text("metadata.source_type", metadata.get("source_type"), MAX_ID_BYTES)
    for field in ("title", "author", "team", "project", "status"):
        value = metadata.get(field)
        if value is not None:
            check_text(f"metadata.{field}", value, MAX_METADATA_VALUE_BYTES)
    labels = metadata.get("labels", [])
    check_count("metadata.labels", len(labels), MAX_LABELS)
    for label in labels:
        check_text("metadata.labels[]", label, MAX_METADATA_VALUE_BYTES)
    connector_map = metadata.get("connector_map")
    if connector_map is not None:
        if not isinstance(connector_map, Mapping):
            raise ContractViolation("metadata.connector_map is not a JSON object")
        size = len(json.dumps(connector_map, separators=(",", ":")).encode())
        if size > MAX_CONNECTOR_MAP_BYTES:
            raise ContractViolation(
                f"metadata.connector_map exceeds {MAX_CONNECTOR_MAP_BYTES} bytes"
            )


def check_document_change(change: Mapping[str, Any]) -> None:
    """One change in its contract (JSON) form."""
    check_text("root_id", change.get("root_id"), MAX_ID_BYTES)
    for field in ("source_revision", "expected_prior_revision", "permission_revision"):
        value = change.get(field)
        if value is not None:
            check_text(field, value, MAX_REVISION_BYTES)
    check_readers("readers", change.get("readers", []))
    check_ids("containers", change.get("containers", []))
    op = change.get("op")
    if op == "delete":
        if change.get("metadata") is not None or change.get("sections") or change.get("attributes"):
            raise ContractViolation("a delete carries no metadata, attributes or sections")
        return
    if op != "upsert":
        raise ContractViolation("op must be upsert or delete")
    if change.get("source_revision") is None:
        raise ContractViolation("an upsert needs source_revision")
    metadata = change.get("metadata")
    if metadata is None:
        raise ContractViolation("an upsert needs metadata")
    check_metadata(metadata)
    sections = change.get("sections", [])
    if not sections:
        raise ContractViolation("an upsert needs at least one section")
    for index, section in enumerate(sections):
        try:
            if not isinstance(section.get("text"), str):
                raise ContractViolation("text is not a string")
            readers = section.get("readers")
            if readers is not None:
                check_readers("readers", readers)
            location = section.get("location")
            if location is not None:
                check_text("location.kind", location.get("kind"), MAX_ID_BYTES)
                label = location.get("label")
                if label is not None:
                    check_text("location.label", label, MAX_METADATA_VALUE_BYTES)
        except ContractViolation as violation:
            raise ContractViolation(f"sections[{index}]: {violation}") from None
    check_attributes(change.get("attributes", []))


def check_document_changes_json(changes: Sequence[Mapping[str, Any]]) -> None:
    """Check content rows and reject repeated document identities."""
    roots = set()
    for index, change in enumerate(changes):
        try:
            check_document_change(change)
        except ContractViolation as violation:
            raise ContractViolation(f"changes[{index}]: {violation}") from None
        if change["root_id"] in roots:
            raise ContractViolation(f"changes[{index}]: a document appears twice in one batch")
        roots.add(change["root_id"])


def check_batch(batch: Mapping[str, Any]) -> None:
    """A whole `ConnectorBatch` document (header and body)."""
    check_text("source_id", batch.get("source_id"), MAX_ID_BYTES)
    cursor = batch.get("cursor") or {}
    if cursor.get("start") is not None:
        check_cursor("cursor.start", cursor["start"])
    check_cursor("cursor.end", cursor.get("end"))
    body = batch.get("body") or {}
    body_type = body.get("type")
    if not isinstance(body_type, str):
        raise ContractViolation("body.type is unknown")
    kind = {
        "document_changes": "content",
        "permission_changes": "permissions",
        "principal_snapshot": "members",
        "listing": "reconcile",
    }.get(body_type)
    if kind is None:
        raise ContractViolation("body.type is unknown")
    if batch.get("kind") != kind:
        raise ContractViolation("body type does not match the kind")
    if kind == "content":
        check_document_changes_json(body.get("changes", []))
    elif kind == "permissions":
        for index, document in enumerate(body.get("documents", [])):
            _at(f"documents[{index}]", _check_permission, document)
        for index, grant in enumerate(body.get("container_grants", [])):
            _at(f"container_grants[{index}]", _check_grant, grant)
    elif kind == "members":
        check_text("snapshot_id", body.get("snapshot_id"), MAX_ID_BYTES)
        for index, user in enumerate(body.get("users", [])):
            _at(f"users[{index}]", _check_user, user)
        for index, group in enumerate(body.get("groups", [])):
            _at(f"groups[{index}]", _check_group, group)
    else:
        check_text("listing_id", body.get("listing_id"), MAX_ID_BYTES)
        if body.get("complete") and not body.get("last"):
            raise ContractViolation("listing: only the final part can be complete")
        check_ids("root_ids", body.get("root_ids", []))


def _at(path: str, check: Any, value: Mapping[str, Any]) -> None:
    try:
        check(value)
    except ContractViolation as violation:
        raise ContractViolation(f"{path}: {violation}") from None


def _check_permission(document: Mapping[str, Any]) -> None:
    check_text("root_id", document.get("root_id"), MAX_ID_BYTES)
    check_text("permission_revision", document.get("permission_revision"), MAX_REVISION_BYTES)
    check_readers("readers", document.get("readers", []))
    check_ids("containers", document.get("containers", []))


def _check_grant(grant: Mapping[str, Any]) -> None:
    check_text("container_id", grant.get("container_id"), MAX_ID_BYTES)
    check_text("container_kind", grant.get("container_kind"), MAX_ID_BYTES)
    check_text("permission_revision", grant.get("permission_revision"), MAX_REVISION_BYTES)
    check_readers("readers", grant.get("readers", []))


def _check_user(user: Mapping[str, Any]) -> None:
    check_text("id", user.get("id"), MAX_ID_BYTES)
    if user.get("email") is not None:
        check_text("email", user["email"], MAX_METADATA_VALUE_BYTES)
    if user.get("email_verification", "unverified") not in (
        "unverified",
        "verified_corporate",
        "verified_personal",
    ):
        raise ContractViolation("invalid email verification evidence")


def _check_group(group: Mapping[str, Any]) -> None:
    check_text("id", group.get("id"), MAX_ID_BYTES)
    check_readers("members", group.get("members", []))
