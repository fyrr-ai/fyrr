"""The provider boundary: discovery, configuration and batch encoding.

`fyrr_connectors.host` serves these functions to fyrr's worker over a socket.
Content rows leave as Arrow record batches (the host sends them as Arrow IPC
streams); other batches use JSON. Provider failures use closed diagnostics,
without exception text or values.
"""

from __future__ import annotations

import dataclasses
import json
import os
import traceback
from collections.abc import Generator, Mapping
from importlib import import_module, metadata
from typing import Any

import pyarrow as pa

from .arrow import rows_to_json
from .config import ConfigError
from .connector import (
    Batch,
    Connector,
    ContentBatch,
    Limits,
    ListingBatch,
    PermissionBatch,
    PrincipalBatch,
    SourceContext,
    SyncKind,
)
from .validate import ContractViolation
from .versions import CONNECTOR_CONTRACT_VERSION, ENTRY_POINT_GROUP, SDK_VERSION

_SDK_DIR = os.path.dirname(os.path.abspath(__file__))
_BATCH_TYPES: dict[SyncKind, type] = {
    SyncKind.CONTENT: ContentBatch,
    SyncKind.PERMISSIONS: PermissionBatch,
    SyncKind.MEMBERS: PrincipalBatch,
    SyncKind.RECONCILE: ListingBatch,
}


class ProviderFailure(Exception):
    """A provider failed; `args[0]` is a value-free JSON description."""


def describe(error: BaseException) -> dict[str, Any]:
    """Exception type and the last code locations, without the message (or,
    for the SDK's own violations, with its value-free message)."""
    frames = traceback.extract_tb(error.__traceback__)
    kind = type(error)
    described: dict[str, Any] = {
        "type": f"{kind.__module__}.{kind.__qualname__}",
        "frames": [
            f"{os.path.basename(frame.filename)}:{frame.lineno} in {frame.name}"
            for frame in frames[-8:]
        ],
    }
    raised_by_sdk = bool(frames) and os.path.dirname(os.path.abspath(frames[-1].filename)) == (
        _SDK_DIR
    )
    if isinstance(error, ContractViolation | ConfigError) and raised_by_sdk:
        described["message"] = str(error)
    return described


def sdk_version() -> str:
    """The installed SDK distribution's version."""
    try:
        return metadata.version("fyrr-connectors")
    except metadata.PackageNotFoundError:
        return SDK_VERSION


def _installed_providers():
    for entry in sorted(
        metadata.entry_points(group=ENTRY_POINT_GROUP), key=lambda e: (e.name, e.value)
    ):
        yield (
            entry,
            {
                "entry_point": entry.name,
                "distribution": entry.dist.name if entry.dist else None,
                "version": entry.dist.version if entry.dist else None,
            },
        )


def inventory() -> str:
    """Entry-point identities only; never invoke provider code."""
    return json.dumps(
        {
            "sdk_version": sdk_version(),
            "providers": [record for _, record in _installed_providers()],
        }
    )


def discover(name: str | None = None) -> str:
    """Installed provider metadata, or a value-free failure, as JSON."""
    providers = []
    for entry, record in _installed_providers():
        if name is not None and entry.name != name:
            continue
        try:
            record["info"] = json.loads(json.dumps(entry.load()()))
        except Exception:
            record["error"] = {"type": "provider_failure"}
        providers.append(record)
    return json.dumps({"sdk_version": sdk_version(), "providers": providers})


def provider_entry_point(name: str) -> metadata.EntryPoint:
    found = [
        entry for entry in metadata.entry_points(group=ENTRY_POINT_GROUP) if entry.name == name
    ]
    if len(found) != 1:
        raise LookupError("expected exactly one provider with this name")
    return found[0]


def connector_class(info: Mapping[str, Any]) -> type[Connector]:
    """Import the connector class named by provider metadata."""
    module_name, _, attribute = str(info["connector"]).partition(":")
    target: Any = import_module(module_name)
    for part in attribute.split("."):
        target = getattr(target, part)
    if not (isinstance(target, type) and issubclass(target, Connector)):
        raise TypeError("the provider's connector is not a Connector subclass")
    return target


def open_connector(
    source: Mapping[str, Any],
    info: Mapping[str, Any] | None = None,
    revision_view: tuple[Any, Any] | None = None,
) -> Connector:
    """Instantiate the connector of one configured source (fyrr's JSON form:
    source_id, provider, permissions, readers, settings, scope, limits). The
    provider's metadata is loaded from its entry point unless given."""
    if info is None:
        info = provider_entry_point(source["provider"]).load()()
    cls = connector_class(info)
    context = SourceContext(
        source_id=source["source_id"],
        permissions=source["permissions"],
        readers=tuple(source.get("readers", ())),
        settings=cls.Settings.parse(source.get("settings"), "settings"),
        scope=cls.Scope.parse(source.get("scope"), "scope"),
        limits=Limits(**source["limits"]),
        lookup_revisions=revision_view[0] if revision_view else None,
        next_revisions=revision_view[1] if revision_view else None,
    )
    return cls(context)


def encode(
    source_id: str, kind: SyncKind, start: str | None, batch: Batch
) -> tuple[dict[str, Any], pa.RecordBatch | None]:
    """The contract header of one batch and, for content, its Arrow rows. The
    header's body holds everything except content rows."""
    expected = _BATCH_TYPES[kind]
    if not isinstance(batch, expected):
        raise ContractViolation(f"a {kind} sync yields {expected.__name__} values")
    header: dict[str, Any] = {
        "schema_version": CONNECTOR_CONTRACT_VERSION,
        "source_id": source_id,
        "kind": str(kind),
        "cursor": {"start": start, "end": batch.cursor},
    }
    rows = None
    if isinstance(batch, ContentBatch):
        if not isinstance(batch.changes, pa.RecordBatch):
            raise ContractViolation("ContentBatch.changes must be a pyarrow.RecordBatch")
        header["body"] = {"type": "document_changes", "changes": []}
        rows = batch.changes
    else:
        tag = {
            SyncKind.PERMISSIONS: "permission_changes",
            SyncKind.MEMBERS: "principal_snapshot",
            SyncKind.RECONCILE: "listing",
        }[kind]
        body = {"type": tag}
        for field in dataclasses.fields(expected):
            if field.name == "cursor":
                continue
            value = getattr(batch, field.name)
            if field.name in ("documents", "container_grants", "users", "groups"):
                value = [_plain(item) for item in value]
            elif field.name == "root_ids":
                value = list(value)
            body[field.name] = value
        header["body"] = body
    return header, rows


def header_bytes(source_id: str, kind: SyncKind, start: str | None, batch: Batch) -> int:
    """The exact size of the header that `run` sends for this batch, in bytes: what a
    provider checks against `limits.max_bytes` before it yields the batch."""
    header, _ = encode(source_id, kind, start, batch)
    return len(json.dumps(header).encode())


def contract_document(header: dict[str, Any], rows: pa.RecordBatch | None) -> dict[str, Any]:
    """The full contract (JSON) form of a batch, as in the golden fixtures."""
    if rows is None:
        return header
    document = dict(header)
    document["body"] = {"type": "document_changes", "changes": rows_to_json(rows)}
    return document


def _plain(value: Any) -> dict[str, Any]:
    plain = dataclasses.asdict(value)
    return {
        key: list(item) if isinstance(item, tuple) else item
        for key, item in plain.items()
        if not (key == "email" and item is None)
        and not (key == "email_verification" and item == "unverified")
    }


def run(
    source_json: str, kind: str, cursor: str | None, revision_view: tuple[Any, Any] | None = None
) -> Generator[tuple[str, Any]]:
    """Run one sync kind of one source; yields (header JSON, rows or None).
    Any provider failure becomes a value-free `ProviderFailure`."""
    source = json.loads(source_json)
    try:
        sync_kind = SyncKind(kind)
        connector = open_connector(source, revision_view=revision_view)
        start = cursor
        for batch in connector.run(sync_kind, cursor):
            header, rows = encode(source["source_id"], sync_kind, start, batch)
            yield json.dumps(header), rows
            start = batch.cursor
    except Exception as error:
        raise ProviderFailure(json.dumps(describe(error))) from None
