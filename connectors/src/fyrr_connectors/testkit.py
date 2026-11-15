"""Validate providers against the bundled schemas with fyrr-connectors[testkit].

`check_provider` checks installed metadata, `run_sync` validates every batch
and cursor, and `assert_golden` compares JSON output with a committed fixture.
Set FYRR_UPDATE_GOLDEN=1 to rewrite goldens. The Rust worker repeats validation.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from importlib.resources import files
from pathlib import Path
from typing import Any

import pyarrow as pa

from . import runtime
from .arrow import check_document_changes, schema_differences
from .arrow import published_document_changes_schema as published_document_changes_schema
from .config import SECRET_MARK
from .connector import Limits, SyncKind
from .runtime import provider_entry_point as provider_entry_point
from .validate import ContractViolation, check_batch
from .versions import SDK_SERIES, sdk_series

#: Defaults of fyrr's `writer.envelope_max_rows`, `envelope_max_bytes` and
#: `document_max_bytes`.
DEFAULT_LIMITS = Limits(max_rows=50_000, max_bytes=256 << 20, document_max_bytes=8 << 20)


def published_schema(name: str) -> dict[str, Any]:
    """A schema published with this SDK (`connector-batch.schema.json`,
    `connector-provider.schema.json`, `connector-document-changes.arrow.json`)."""
    return json.loads((files("fyrr_connectors") / "schemas" / name).read_text())


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _validator(schema: Mapping[str, Any]) -> Any:
    try:
        from jsonschema import Draft202012Validator
    except ImportError as error:  # pragma: no cover - depends on the extra
        raise ImportError("install fyrr-connectors[testkit] for the provider test kit") from error
    return Draft202012Validator(schema)


def _schema_errors(schema: Mapping[str, Any], instance: Any) -> list[str]:
    return sorted(
        f"{'/'.join(map(str, error.absolute_path)) or '<root>'}: {error.validator}"
        for error in _validator(schema).iter_errors(instance)
    )


def check_provider(name: str) -> dict[str, Any]:
    """Load the installed provider `name` and check its metadata."""
    info = provider_entry_point(name).load()()
    check_provider_info(info, entry_point=name)
    return info


def check_provider_info(info: Mapping[str, Any], entry_point: str | None = None) -> None:
    """Check provider metadata against the published schema and this SDK."""
    errors = _schema_errors(published_schema("connector-provider.schema.json"), info)
    _require(not errors, f"provider metadata does not match the published schema: {errors}")
    _require(
        entry_point is None or info["name"] == entry_point,
        "the metadata name must equal the entry point name",
    )
    minimum = tuple(int(part) for part in info["min_sdk_version"].split("."))
    _require(
        sdk_series(info["min_sdk_version"]) == SDK_SERIES,
        f"min_sdk_version must be in SDK series {SDK_SERIES}",
    )
    installed = tuple(int(part) for part in runtime.sdk_version().split(".")[:3])
    _require(minimum <= installed, "min_sdk_version is newer than the installed SDK")
    for field_name in ("connection_types", "sync_kinds"):
        values = info[field_name]
        _require(values and len(set(values)) == len(values), f"{field_name} empty or duplicated")
    _require("content" in info["sync_kinds"], "sync_kinds must include content")
    cls = runtime.connector_class(info)
    _require(
        info["sync_kinds"] == [str(kind) for kind in cls.sync_kinds()],
        "sync_kinds must match the connector's overridden methods",
    )
    for part in ("settings", "scope"):
        schema = info["config_schema"][part]
        _validator(schema).check_schema(schema)
        _require(schema.get("type") == "object", f"config_schema.{part} must be an object schema")
        for path, prop in _secret_properties(schema):
            validator = _validator(schema).evolve(schema=prop)
            _require(
                not list(validator.iter_errors({"env": "FYRR_TESTKIT"})),
                f"config_schema.{part}.{path} is secret and must accept {{ env = ... }}",
            )
            _require(
                bool(list(validator.iter_errors("literal secret"))),
                f"config_schema.{part}.{path} is secret and must refuse literal values",
            )


def _secret_properties(schema, prefix="", *, root=None, depth=0) -> list[tuple[str, Any]]:
    """Bounded traversal of local references, object properties and array items."""
    if root is None:

        def contains_secret(node):
            if isinstance(node, Mapping):
                return node.get(SECRET_MARK) is True or any(
                    contains_secret(v) for v in node.values()
                )
            return isinstance(node, list) and any(contains_secret(v) for v in node)

        if not contains_secret(schema):
            return []
        root = schema
    _require(
        depth <= 64 and isinstance(schema, Mapping | bool),
        "secret schema traversal is invalid or exceeds 64 levels",
    )
    if isinstance(schema, bool):
        return []
    if schema.get(SECRET_MARK) is True:
        return [(prefix, schema)]
    found = []
    if "$ref" in schema:
        reference = schema["$ref"]
        _require(
            isinstance(reference, str) and (reference == "#" or reference.startswith("#/")),
            "secret schema references must be local JSON pointers",
        )
        target = root
        try:
            for part in reference.split("/")[1:]:
                part = part.replace("~1", "/").replace("~0", "~")
                target = target[int(part)] if isinstance(target, list) else target[part]
        except (KeyError, IndexError, TypeError, ValueError):
            raise AssertionError("secret schema reference does not resolve") from None
        found.extend(_secret_properties(target, prefix, root=root, depth=depth + 1))
    for keyword in (
        "allOf",
        "anyOf",
        "oneOf",
        "not",
        "if",
        "then",
        "else",
        "dependentSchemas",
        "patternProperties",
        "prefixItems",
        "contains",
        "$dynamicRef",
    ):
        _require(keyword not in schema, f"secret schema traversal does not support {keyword}")
    for keyword in ("additionalProperties", "unevaluatedProperties", "unevaluatedItems"):
        _require(
            not isinstance(schema.get(keyword), Mapping),
            f"secret schema traversal does not support {keyword} schemas",
        )
    for name, prop in (schema.get("properties") or {}).items():
        path = f"{prefix}.{name}" if prefix else name
        found.extend(_secret_properties(prop, path, root=root, depth=depth + 1))
    if "items" in schema:
        found.extend(_secret_properties(schema["items"], f"{prefix}[]", root=root, depth=depth + 1))
    return found


def check_config(
    info: Mapping[str, Any],
    settings: Mapping[str, Any] | None = None,
    scope: Mapping[str, Any] | None = None,
) -> None:
    """Check a source's settings and scope against the provider's config
    schema, as `fyrr config check` does."""
    for part, value in (("settings", settings or {}), ("scope", scope or {})):
        errors = _schema_errors(info["config_schema"][part], value)
        _require(not errors, f"{part} does not match the provider's config schema: {errors}")


@dataclass
class SyncResult:
    """The batches of one sync kind in their contract (JSON) form, the Arrow
    content rows and the cursor after the last batch."""

    kind: SyncKind
    documents: list[dict[str, Any]] = field(default_factory=list)
    record_batches: list[pa.RecordBatch] = field(default_factory=list)
    cursor: str | None = None


def run_sync(
    provider: str | Mapping[str, Any],
    kind: str,
    *,
    settings: Mapping[str, Any] | None = None,
    scope: Mapping[str, Any] | None = None,
    source_id: str = "testkit-source",
    permissions: str | None = None,
    readers: Sequence[str] = ("grp:testkit",),
    cursor: str | None = None,
    limits: Limits = DEFAULT_LIMITS,
) -> SyncResult:
    """Run one sync kind and check every batch. `provider` is an installed
    provider's name or its metadata (the callable's result, before
    installation). Provider exceptions propagate unchanged."""
    if isinstance(provider, str):
        info = check_provider(provider)
    else:
        info = dict(provider)
        check_provider_info(info)
    check_config(info, settings, scope)
    sync_kind = SyncKind(kind)
    _require(str(sync_kind) in info["sync_kinds"], f"the provider does not implement {kind}")
    connector = runtime.open_connector(
        {
            "source_id": source_id,
            "provider": info["name"],
            "permissions": permissions or info["connection_types"][0],
            "readers": list(readers),
            "settings": dict(settings or {}),
            "scope": dict(scope or {}),
            "limits": asdict(limits),
        },
        info,
    )
    batch_schema = published_schema("connector-batch.schema.json")
    arrow_contract = published_document_changes_schema()
    result = SyncResult(kind=sync_kind, cursor=cursor)
    for index, batch in enumerate(connector.run(sync_kind, cursor)):
        header, rows = runtime.encode(source_id, sync_kind, result.cursor, batch)
        if rows is not None:
            _require(rows.num_rows <= limits.max_rows, f"batch {index} exceeds max_rows")
            _require(
                rows.get_total_buffer_size() <= limits.max_bytes,
                f"batch {index} exceeds max_bytes",
            )
            differences = schema_differences(arrow_contract, rows.schema)
            _require(not differences, f"batch {index}: Arrow schema differs: {differences}")
            check_document_changes(rows)
            result.record_batches.append(rows)
        document = runtime.contract_document(header, rows)
        errors = _schema_errors(batch_schema, document)
        _require(not errors, f"batch {index} does not match the published schema: {errors}")
        try:
            check_batch(document)
        except ContractViolation as violation:
            raise AssertionError(f"batch {index}: {violation}") from None
        result.documents.append(document)
        result.cursor = batch.cursor
    return result


def assert_golden(result: SyncResult, path: Path) -> None:
    """Compare the contract form of a sync with a committed golden file;
    `FYRR_UPDATE_GOLDEN=1` rewrites it."""
    text = json.dumps(result.documents, indent=2, ensure_ascii=False) + "\n"
    if os.environ.get("FYRR_UPDATE_GOLDEN"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return
    _require(path.exists(), f"{path} is missing; create it with FYRR_UPDATE_GOLDEN=1")
    _require(
        json.loads(path.read_text()) == result.documents,
        f"output differs from {path}; review and rewrite with FYRR_UPDATE_GOLDEN=1",
    )
