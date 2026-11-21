"""Connector providers for fyrr's CLI and privacy sentinel tests.

Installed by putting this directory on PYTHONPATH (the hand-written
dist-info registers the entry points). Every value a test wants to keep out of
fyrr's output comes from the JSON fixture named by the source's
`scope.fixture`, never from this code or the configuration, so the markers
appear only where a provider hands them to fyrr.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pyarrow as pa

from fyrr_connectors import (
    ConfigModel,
    Connector,
    ContentBatch,
    DocumentPermission,
    ListingBatch,
    Location,
    Metadata,
    PermissionBatch,
    PrincipalBatch,
    Secret,
    Section,
    SourceGroup,
    SourceUser,
    Upsert,
    document_changes,
    provider_info,
    secret,
    setting,
)
from fyrr_connectors.arrow import DOCUMENT_CHANGES_SCHEMA


class FixtureScope(ConfigModel):
    """Where the test's documents are."""

    fixture: str = setting(description="Path of a JSON file with the documents.")


def _fixture(connector: Connector) -> dict:
    return json.loads(Path(connector.context.scope.fixture).read_text())


class DocsConnector(Connector):
    """Valid output carrying the fixture's markers in every field."""

    Scope = FixtureScope

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        data = _fixture(self)
        changes = [
            Upsert(
                root_id=document["id"],
                source_revision=document.get("revision", "r1"),
                readers=document["readers"],
                metadata=Metadata(
                    source_uri=document.get("uri", f"test://{document['id']}"),
                    source_type="test.doc",
                    title=document["title"],
                    author=document["author"],
                    project=document.get("project"),
                    labels=document.get("labels", []),
                    updated_at=(
                        datetime.fromisoformat(document["updated_at"])
                        if document.get("updated_at")
                        else None
                        if "updated_at" in document
                        else datetime(2026, 9, 30, tzinfo=UTC)
                    ),
                    connector_map={"note": document.get("note", document["text"])},
                ),
                attributes=[f"test.label={document['title']}"],
                sections=[
                    Section(
                        text=section["text"],
                        readers=section["readers"],
                        location=Location(**section["location"]),
                    )
                    for section in document["sections"]
                ]
                if "sections" in document
                else [
                    Section(
                        text=document.get("first_text", document["text"]),
                        location=Location(kind="heading", label=document["title"]),
                        readers=document.get("first_readers"),
                    ),
                    Section(
                        text=document["text"],
                        readers=document["readers"],
                        location=Location(**document["body_location"])
                        if "body_location" in document
                        else None,
                    ),
                ],
            )
            for document in data["documents"]
        ]
        yield ContentBatch(changes=document_changes(changes), cursor=data["cursor"])

    def permissions(self, cursor: str | None) -> Iterator[PermissionBatch]:
        data = _fixture(self)
        yield PermissionBatch(
            documents=[
                DocumentPermission(root_id=d["id"], permission_revision="p1", readers=d["readers"])
                for d in data["documents"]
            ],
            cursor=data["cursor"],
        )

    def members(self, cursor: str | None) -> Iterator[PrincipalBatch]:
        data = _fixture(self)
        yield PrincipalBatch(
            snapshot_id="snapshot",
            part=0,
            last=True,
            users=[
                SourceUser(
                    id=user["id"],
                    email=user["email"],
                    active=True,
                    email_verification=user.get("email_verification", "unverified"),
                )
                for user in data["users"]
            ],
            groups=[
                SourceGroup(id="group", members=[f"usr:{user['id']}" for user in data["users"]])
            ],
            cursor=data["cursor"],
        )

    def listing(self, cursor: str | None) -> Iterator[ListingBatch]:
        data = _fixture(self)
        yield ListingBatch(
            listing_id="listing",
            part=0,
            last=True,
            complete=True,
            root_ids=[document["id"] for document in data["documents"]],
            cursor=data["cursor"],
        )


class InvalidSettings(ConfigModel):
    variant: Literal[
        "readers",
        "attributes",
        "root-id",
        "null-text",
        "null-readers",
        "large-string",
        "duplicate",
        "cursor",
    ] = setting(description="Which contract rule the output breaks.")


class InvalidConnector(Connector):
    """Contract-breaking output whose bad values are the fixture's markers.
    Rows are built with pyarrow directly, bypassing the SDK's checks, as a
    careless provider would."""

    Settings = InvalidSettings
    Scope = FixtureScope

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        data = _fixture(self)
        document = data["documents"][0]
        marker = document["text"]
        row = {
            "op": "upsert",
            "root_id": document["id"],
            "source_revision": "r1",
            "readers": document["readers"],
            "containers": [],
            "source_uri": "test://x",
            "source_type": "test.doc",
            "title": document["title"],
            "labels": [],
            "attributes": [],
            "sections": [{"text": marker, "readers": None}],
        }
        variant = self.context.settings.variant
        schema = DOCUMENT_CHANGES_SCHEMA
        rows = [row]
        if variant == "readers":
            row["readers"] = [f"bad:{marker}"]
        elif variant == "attributes":
            row["attributes"] = [marker]
        elif variant == "root-id":
            row["root_id"] = f" {marker}"
        elif variant == "null-text":
            row["sections"] = [{"text": None}]
        elif variant == "null-readers":
            row["sections"] = [{"text": marker, "readers": [None]}]
        elif variant == "large-string":
            schema = schema.set(1, pa.field("root_id", pa.large_string(), nullable=False))
        elif variant == "duplicate":
            rows = [row, dict(row)]
        batch = pa.RecordBatch.from_pylist(rows, schema=schema)
        end = marker if variant == "cursor" else data["cursor"]
        start_breaks = variant == "cursor"
        yield ContentBatch(changes=batch, cursor=end)
        if start_breaks:
            yield ContentBatch(
                changes=pa.RecordBatch.from_pylist([], schema=DOCUMENT_CHANGES_SCHEMA),
                cursor="",
            )


class FailingSettings(ConfigModel):
    mode: Literal["raise", "abort", "exit", "hang", "print", "memory", "die"] = setting(
        description="How the provider fails."
    )


class FailingConnector(Connector):
    """Fails in the way the settings choose; messages carry the markers."""

    Settings = FailingSettings
    Scope = FixtureScope

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        data = _fixture(self)
        marker = data["documents"][0]["text"]
        mode = self.context.settings.mode
        pid_dir = os.environ.get("FYRR_TEST_PID_DIR")
        if pid_dir:
            # Lets a test prove that no provider process outlives the command.
            Path(pid_dir, f"{mode}.pid").write_text(str(os.getpid()))
        if mode == "raise":
            raise ValueError(f"cannot parse document {marker}")
        if mode == "abort":
            os.abort()
        if mode == "exit":
            raise SystemExit(f"giving up on {marker}")
        if mode == "hang":
            time.sleep(3600)
        if mode == "memory":
            # Far above any worker memory limit the tests configure.
            try:
                hoard = [bytearray(1 << 30) for _ in range(64)]
            except MemoryError:
                if pid_dir:
                    Path(pid_dir, "memory.limited").write_text("1")
                raise
            raise ValueError(len(hoard))
        if mode == "die":
            os._exit(7)
        print("provider stdout line")
        yield ContentBatch(changes=document_changes([]), cursor="printed")


class SecretSettings(ConfigModel):
    """A secret under a key fyrr's own heuristic does not recognize."""

    access: Secret = secret(description="Access credential.")
    region: str = setting(description="Region.", default="eu")


class SecretConnector(Connector):
    Settings = SecretSettings
    Scope = FixtureScope

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        self.context.settings.access.reveal()
        yield ContentBatch(changes=document_changes([]), cursor="done")


def docs_info() -> dict:
    return provider_info(name="sentinel-docs", connector=DocsConnector, min_sdk_version="0.1.0")


def invalid_info() -> dict:
    return provider_info(
        name="sentinel-invalid", connector=InvalidConnector, min_sdk_version="0.1.0"
    )


def failing_info() -> dict:
    return provider_info(
        name="sentinel-failing", connector=FailingConnector, min_sdk_version="0.1.0"
    )


def secret_info() -> dict:
    return provider_info(name="sentinel-secret", connector=SecretConnector, min_sdk_version="0.1.0")


def future_sdk_info() -> dict:
    return provider_info(
        name="sentinel-future-sdk", connector=DocsConnector, min_sdk_version="0.2.0"
    )


def bad_metadata_info() -> dict:
    marker = os.environ.get("FYRR_TEST_METADATA_MARKER", "marker")
    info = provider_info(
        name="sentinel-bad-metadata", connector=DocsConnector, min_sdk_version="0.1.0"
    )
    info["connection_types"] = [marker]
    info["description"] = f"bad\n{marker}"
    return info


def bad_required_info() -> dict:
    """A provider whose config schema names a required property with a marker,
    which fyrr must not print when the source leaves it out."""
    marker = os.environ.get("FYRR_TEST_METADATA_MARKER", "marker")
    info = provider_info(
        name="sentinel-bad-required", connector=DocsConnector, min_sdk_version="0.1.0"
    )
    info["config_schema"]["scope"]["required"] = ["fixture", f"{marker}\nsecond-line"]
    return info


def versioned_info() -> dict:
    """Registered by a distribution whose version is not a version (its
    METADATA says `9.9 SENTINEL-VERSION`)."""
    return provider_info(
        name="sentinel-versioned", connector=DocsConnector, min_sdk_version="0.1.0"
    )


def raising_metadata_info() -> dict:
    if os.environ.get("FYRR_TEST_METADATA_ABORT"):
        os.abort()
    raise RuntimeError(os.environ.get("FYRR_TEST_METADATA_MARKER", "marker"))


def _worker_result_name() -> str:
    """The name of the worker's result channel. The worker keeps it in its own
    environment, which its provider host does not inherit; a hostile provider
    reads it from its parent, as this does, and speaks on the channel."""
    environ = Path(f"/proc/{os.getppid()}/environ").read_bytes().split(b"\0")
    entry = next(e for e in environ if e.startswith(b"FYRR_WORKER_RESULT="))
    return entry.split(b"=", 1)[1].decode()


def _descriptors() -> list[dict]:
    """The open descriptors of this process: number, target, close-on-exec."""
    found = []
    for name in sorted(os.listdir("/proc/self/fd"), key=int):
        try:
            target = os.readlink(f"/proc/self/fd/{name}")
            info = Path(f"/proc/self/fdinfo/{name}").read_text()
        except OSError:
            continue  # the descriptor of the listing itself
        flags = int(
            next(line for line in info.splitlines() if line.startswith("flags:")).split()[1], 8
        )
        found.append({"fd": int(name), "target": target, "cloexec": bool(flags & os.O_CLOEXEC)})
    return found


def _forked_descriptors(path: str) -> list[dict]:
    """What a process this provider forks (`os.fork`, which no close-on-exec
    flag covers) holds."""
    pid = os.fork()
    if pid == 0:
        try:
            Path(path + ".fork").write_text(json.dumps(_descriptors()))
        finally:
            os._exit(0)
    os.waitpid(pid, 0)
    return json.loads(Path(path + ".fork").read_text())


def _report_descriptors(path: str) -> None:
    """What this provider, a child it starts (without closing descriptors, as a
    careless provider would) and a process it forks hold, for the test to read."""
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json, os; d = '/proc/self/fd/'; print(json.dumps([[int(n), os.readlink(d + n)] "
            "for n in os.listdir(d) if os.path.lexists(d + n)]))",
        ],
        close_fds=False,
        capture_output=True,
        check=True,
    )
    internal = ("FYRR_WORKER_RESULT", "FYRR_WORKER_MEMORY", "FYRR_WRITER_EMBEDDING_CAP")
    Path(path).write_text(
        json.dumps(
            {
                "provider": _descriptors(),
                "forked": _forked_descriptors(path),
                "child": json.loads(child.stdout),
                "inherited_internal": any(name in os.environ for name in internal),
            }
        )
    )


class ReviewSettings(ConfigModel):
    mode: str = setting(description="Review reproduction.")


class ReviewConnector(Connector):
    Settings = ReviewSettings
    Scope = FixtureScope

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        data = _fixture(self)
        marker = data["documents"][0]["text"]
        mode = self.context.settings.mode
        if mode == "print":
            print(marker, flush=True)
            print(marker, file=sys.stderr, flush=True)
            os.write(1, marker.encode())
            os.write(2, marker.encode())
        if mode in ("forge", "forge-error"):
            os.write(
                1,
                json.dumps(
                    {"kinds": [], "error": marker if mode == "forge-error" else None}
                ).encode()
                + b"\n",
            )
            os._exit(17)
        if mode == "fds":
            _report_descriptors(data["fd_report"])
        if mode.startswith("child-"):
            child = subprocess.Popen(
                ["/bin/sleep", "60"], start_new_session=data.get("detached", False)
            )
            Path(data["pid_file"]).write_text(str(child.pid))
            if mode == "child-exit":
                os._exit(17)
            if mode == "child-error":
                raise ValueError(marker)
            if mode == "child-parent-error":
                channel = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                channel.connect("\0" + _worker_result_name())
                channel.sendall(b"x" * ((1 << 20) + 1))
                time.sleep(60)
            if mode == "child-timeout":
                time.sleep(60)
        if mode.startswith("result-"):
            payload = {"kinds": [], "error": None}
            if mode == "result-error":
                payload["error"] = marker
            if mode == "result-kinds":
                payload = {
                    "kinds": [
                        {
                            "kind": "reconcile",
                            "stats": {
                                key: 0
                                for key in [
                                    "batches",
                                    "upserts",
                                    "deletes",
                                    "sections",
                                    "text_bytes",
                                    "oversized",
                                    "permission_documents",
                                    "container_grants",
                                    "users",
                                    "groups",
                                    "listed_root_ids",
                                ]
                            }
                            | {"listing_complete": False, "cursor_advanced": False},
                            "error": None,
                        }
                    ],
                    "error": None,
                }
            channel = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            channel.connect("\0" + _worker_result_name())
            channel.sendall(json.dumps(payload).encode())
            os._exit(0)
        if mode == "allocate":
            bytearray(3 << 30)
        if mode == "timeout-resume" and cursor == "done":
            return
        schema = DOCUMENT_CHANGES_SCHEMA
        row = {
            "op": "upsert",
            "root_id": "doc",
            "source_revision": "r1",
            "readers": ["grp:testers"],
            "containers": [],
            "source_uri": "test://doc",
            "source_type": "test",
            "labels": [],
            "attributes": [],
            "sections": [{"text": "ok"}],
        }
        if mode == "extra-name":
            schema = schema.append(pa.field(marker, pa.string(), nullable=False))
            row[marker] = "extra"
        if mode == "extra-huge":
            schema = schema.with_metadata({**schema.metadata, b"fyrr.schema_version": b"1.1"})
            schema = schema.append(pa.field("extension", pa.binary(), nullable=True))
            row["extension"] = b"x" * (64 << 20)
        if mode == "text-huge":
            row["sections"] = [{"text": "x" * (64 << 20)}]
        yield ContentBatch(changes=pa.RecordBatch.from_pylist([row], schema=schema), cursor="done")
        if mode == "timeout-resume":
            time.sleep(60)


def review_info() -> dict:
    return provider_info(name="sentinel-review", connector=ReviewConnector, min_sdk_version="0.1.0")


class Account(ConfigModel):
    access: Secret = secret(description="Account credential.")


class NestedSettings(ConfigModel):
    accounts: list[Account] = setting(description="Accounts.")
    accesses: list[Secret] = setting(description="Scalar credentials.", default_factory=list)


class NestedConnector(SecretConnector):
    Settings = NestedSettings

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        self.context.settings.accounts[0].access.reveal()
        yield ContentBatch(changes=document_changes([]), cursor="done")


def nested_info() -> dict:
    return provider_info(name="sentinel-nested", connector=NestedConnector, min_sdk_version="0.1.0")
