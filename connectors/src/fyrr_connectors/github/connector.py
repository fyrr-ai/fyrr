"""Bundled GitHub provider: fixed content and deletion-safe full-ID listing."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import replace
from typing import Any

from ..arrow import Delete, Metadata, Section, Upsert, document_changes
from ..connector import (
    Connector,
    ContainerGrant,
    ContentBatch,
    DocumentPermission,
    ListingBatch,
    PermissionBatch,
    PrincipalBatch,
    SyncKind,
)
from ..validate import ContractViolation
from . import render
from .auth import AppAuth
from .client import MAX_ENTRIES, ApiError, Client, ResponseLimit, instant, object_id
from .config import RepositoryScope, Scope, Settings
from .feeds import (
    ConversationRef,
    Cursor,
    FeedKind,
    RepoCursor,
    Repository,
    comment_hints,
    conversations,
    inventory,
    refresh_budget,
)
from .permissions import AccessSnapshot, container
from .tree import Tree

QUARANTINE = (Section(text="\0"),)


class GitHub(Connector[Settings, Scope]):
    Settings = Settings
    Scope = Scope

    def _open(self):
        settings, scope = self.context.settings, self.context.scope
        assert isinstance(settings, Settings) and isinstance(scope, Scope)
        if self.context.permissions == "fixed" and not self.context.readers:
            raise ValueError("github_fixed_readers_required")
        if self.context.permissions not in ("fixed", "sync"):
            raise ValueError("github_permissions_invalid")
        self.client = Client(settings.api_endpoint)
        self.auth = AppAuth(self.client, settings)
        self.tree = Tree(self.client)
        self.installed = inventory(self.client)
        self.scope = scope
        self.repos = {
            item.id: Repository.read(self.client, item.id)
            for item in scope.repositories
            if item.id in self.installed
        }
        if any(repo.owner_id != self.auth.installation.account_id for repo in self.repos.values()):
            raise ValueError("github_repository_account_mismatch")
        if self.context.permissions == "sync" and any(
            repo.visibility != "private" for repo in self.repos.values()
        ):
            raise ValueError("github_sync_private_only")
        self.known: dict[str, str] = {}
        if self.context.next_revisions:
            while entries := self.context.next_revisions():
                self.known.update(entries)
                if len(self.known) > MAX_ENTRIES:
                    raise ValueError("github_revision_view_limit")

    def _lookup(self, root: str) -> str | None:
        if root not in self.known and self.context.lookup_revisions:
            self.known.update(self.context.lookup_revisions([root]))
        return self.known.get(root)

    def _missing_roots(self, observed: set[int]):
        known_repos = {ref.repo_id for root in self.known if (ref := ConversationRef.decode(root))}
        for item in self.scope.repositories:
            if item.id not in self.installed and item.id not in observed | known_repos:
                # Even a files-only source has stable repository prefixes.
                prefix = f"github:repo:{item.id}:"
                if not any(root.startswith(prefix) for root in self.known):
                    raise ValueError("github_repository_not_installed")

    def _parent(self, repo: Repository, ref: ConversationRef) -> dict[str, Any]:
        endpoint = "pulls" if ref.kind == FeedKind.PR else "issues"
        parent = self.client.get(repo.path + f"/{endpoint}/{ref.number}")
        repo.verify(self.client)
        if object_id(parent) != ref.id or parent.get("number") != ref.number:
            raise ValueError("github_object_identity_changed")
        # GitHub list payloads are hints. The fetched resource's repository
        # binding is mandatory before any body is ingested.
        if ref.kind == FeedKind.PR:
            base = parent.get("base")
            owner = base.get("repo") if isinstance(base, dict) else None
            if not isinstance(owner, dict) or object_id(owner) != repo.id:
                raise ValueError("github_object_parent_changed")
        elif parent.get("repository_url") != self.client.origin + repo.path:
            raise ValueError("github_object_parent_changed")
        return parent

    def _part(self, repo: Repository, ref: ConversationRef, entry: dict[str, Any], kind: str):
        object_id(entry)
        endpoint = "issues" if kind == "comment" else "pulls"
        key = "issue_url" if kind == "comment" else "pull_request_url"
        if entry.get(key) != self.client.origin + repo.path + f"/{endpoint}/{ref.number}":
            raise ValueError("github_child_parent_changed")
        keys = (
            "id",
            "body",
            "created_at",
            "updated_at",
            "submitted_at",
            "state",
            "path",
            "line",
            "original_line",
        )
        return {**{key: entry[key] for key in keys if key in entry}, "kind": kind}

    def _conversation(
        self, repo: Repository, ref: ConversationRef, before: dict[str, Any]
    ) -> render.Rendered:
        previous = None
        for _ in range(3):
            current = self._conversation_once(repo, ref, before)
            if previous is not None and current.revision == previous.revision:
                return current
            previous = current
        raise ValueError("github_children_changed_during_read")

    def _conversation_once(
        self, repo: Repository, ref: ConversationRef, before: dict[str, Any]
    ) -> render.Rendered:
        parts: list[dict[str, Any]] = []
        text_bytes = len(render.opening(before, self.context.limits.document_max_bytes).encode())
        metadata_bytes = 0

        def keep(part: dict[str, Any]):
            nonlocal text_bytes, metadata_bytes
            text_bytes += len(
                render.text(part.get("body"), self.context.limits.document_max_bytes).encode()
            )
            metadata_bytes += len(json.dumps(part).encode())
            if (
                text_bytes > self.context.limits.document_max_bytes
                or metadata_bytes > self.context.limits.max_bytes
                or len(parts) >= MAX_ENTRIES
            ):
                raise render.InvalidDocument
            parts.append(part)

        for entries in self.client.pages(repo.path + f"/issues/{ref.number}/comments"):
            for entry in entries:
                keep(self._part(repo, ref, entry, "comment"))
        if ref.kind == FeedKind.PR:
            submitted: set[int] = set()
            for entries in self.client.pages(repo.path + f"/pulls/{ref.number}/reviews"):
                for entry in entries:
                    review_id = object_id(entry)
                    if entry.get("state") == "PENDING":
                        continue
                    if (
                        entry.get("state")
                        not in (
                            "APPROVED",
                            "CHANGES_REQUESTED",
                            "COMMENTED",
                            "DISMISSED",
                        )
                        or instant(entry.get("submitted_at")) is None
                    ):
                        raise ValueError("github_review_invalid")
                    submitted.add(review_id)
                    keep(self._part(repo, ref, entry, "review"))
            for entries in self.client.pages(repo.path + f"/pulls/{ref.number}/comments"):
                for entry in entries:
                    object_id(entry)
                    comment_review_id = entry.get("pull_request_review_id")
                    if comment_review_id is not None and (
                        not isinstance(comment_review_id, int)
                        or isinstance(comment_review_id, bool)
                        or comment_review_id <= 0
                    ):
                        raise ValueError("github_review_invalid")
                    if comment_review_id in submitted:
                        keep(self._part(repo, ref, entry, "inline"))
        if len(parts) > MAX_ENTRIES or len({(part["kind"], part["id"]) for part in parts}) != len(
            parts
        ):
            raise ValueError("github_children_invalid")
        after = self._parent(repo, ref)
        if before != after:
            raise ValueError("github_object_changed_during_read")
        return render.conversation(repo, ref, after, parts, self.context.limits.document_max_bytes)

    def _upsert(self, root: str, rendered: render.Rendered) -> Upsert:
        return Upsert(
            root_id=root,
            source_revision=rendered.revision,
            metadata=rendered.metadata,
            sections=rendered.sections,
            readers=self.context.readers if self.context.permissions == "fixed" else (),
        )

    def _bad(self, root: str, repo: Repository, kind: str, identity: object) -> Upsert:
        revision = hashlib.sha256(f"invalid:{render.FORMAT}:{identity}".encode()).hexdigest()
        return Upsert(
            root_id=root,
            source_revision=revision,
            metadata=Metadata(
                source_uri=f"https://github.com/{repo.name}", source_type=kind, project=repo.name
            ),
            sections=QUARANTINE,
            readers=self.context.readers if self.context.permissions == "fixed" else (),
        )

    def _read(self, repo: Repository, ref: ConversationRef) -> Upsert | Delete | None:
        try:
            before = self._parent(repo, ref)
        except ApiError as error:
            if error.status != 404 and not error.moved:
                raise
            if Repository.read(self.client, repo.id) != repo:
                raise ValueError("github_repository_moved_during_read") from None
            if self._lookup(ref.root):
                return Delete(root_id=ref.root, source_revision="absent")
            return None
        try:
            rendered = self._conversation(repo, ref, before)
        except (render.InvalidDocument, ResponseLimit):
            return self._bad(ref.root, repo, "github-conversation", [ref.id, ref.number])
        return self._upsert(ref.root, rendered)

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        self._open()
        state = Cursor.decode(cursor, self.scope)
        self._missing_roots(state.observed)
        state.repos = {key: value for key, value in state.repos.items() if key in self.installed}
        durable = cursor or Cursor(state.fingerprint).encode()
        pending: dict[str, Upsert | Delete] = {}
        limits = self.context.limits

        def flush(frontier: str) -> ContentBatch:
            batch = ContentBatch(changes=document_changes(pending.values()), cursor=frontier)
            pending.clear()
            return batch

        def hand(change: Upsert | Delete | None):
            if change is None:
                return
            known = self._lookup(change.root_id)
            if isinstance(change, Upsert) and change.source_revision == known:
                return
            if isinstance(change, Upsert):
                # A document the contract rejects, or that is too large, is
                # replaced by the quarantine marker and keeps its revision.
                try:
                    rejected = document_changes([change]).get_total_buffer_size() > limits.max_bytes
                except ContractViolation:
                    rejected = True
                if rejected:
                    change = replace(
                        change,
                        sections=QUARANTINE,
                        metadata=Metadata(
                            source_uri="https://github.com", source_type="github-document"
                        ),
                    )
            if document_changes([change]).get_total_buffer_size() > limits.max_bytes:
                raise ValueError("github_batch_limit")
            candidate = {**pending, change.root_id: change}
            if pending and (
                len(candidate) > limits.max_rows
                or document_changes(candidate.values()).get_total_buffer_size() > limits.max_bytes
            ):
                yield flush(durable)
            pending[change.root_id] = change
            if isinstance(change, Upsert):
                self.known[change.root_id] = change.source_revision
            else:
                self.known.pop(change.root_id, None)

        for item in self.scope.repositories:
            if item.id not in self.repos:
                continue
            repo = self.repos[item.id]
            previous = state.repos.get(item.id, RepoCursor())
            frontier = self.client.server_time.isoformat()
            refs = list(conversations(self.client, repo, item, previous.since))
            if len(refs) > MAX_ENTRIES:
                raise ValueError("github_conversation_limit")
            # Hints are not deduplicated: a later canonical read can contradict an earlier one.
            for ref in refs:
                yield from hand(self._read(repo, ref))
            for ref in comment_hints(self.client, repo, item, previous.since):
                yield from hand(self._read(repo, ref))
            refresh = sorted(
                (
                    ref
                    for root in list(self.known)
                    if (ref := ConversationRef.decode(root)) is not None
                    and ref.repo_id == item.id
                    and (
                        (ref.kind == FeedKind.ISSUE and item.issues)
                        or (ref.kind == FeedKind.PR and item.pull_requests)
                    )
                ),
                key=lambda ref: (ref.id, int(ref.kind == FeedKind.PR)),
            )
            rotate = previous.rotate
            now = int(self.client.server_time.timestamp())
            elapsed = (
                max(1, now - previous.refresh_step) if previous.refresh_step is not None else 900
            )
            budget = refresh_budget(len(refresh), elapsed)
            remaining = [ref for ref in refresh if (ref.id, int(ref.kind == FeedKind.PR)) > rotate]
            # Fill the budget across the wrap: a short tail must not add a poll.
            candidates = (remaining + refresh[: max(0, budget - len(remaining))])[:budget]
            for ref in candidates:
                yield from hand(self._read(repo, ref))
            completed = budget >= len(remaining)
            next_rotate = (
                (candidates[-1].id, int(candidates[-1].kind == FeedKind.PR))
                if candidates and budget != len(remaining)
                else (0, 0)
            )
            commit = None
            if item.prose or item.code:
                commit = self.tree.head(repo, item)
                files = list(self.tree.files(repo, item, commit))
                current = {ref.root for ref in files}
                repository_revision = hashlib.sha256(
                    json.dumps([render.FORMAT, repo.name, repo.visibility, repo.archived]).encode()
                ).hexdigest()[:32]
                acquired = []
                for file in files:
                    stamp = f"{file.blob}:{repository_revision}:{file.commit}"
                    known = self._lookup(file.root)
                    if known and re.fullmatch(r"[0-9a-f]{64}:" + re.escape(stamp), known):
                        continue
                    try:
                        if file.size > limits.document_max_bytes:
                            raise render.InvalidDocument
                        body = self.tree.blob(repo, file)
                        commits = self.client.pages(
                            repo.path + "/commits", {"sha": commit, "path": file.path}
                        )
                        latest = next(commits, [])
                        rendered = render.file(
                            repo,
                            file,
                            body,
                            latest[0] if latest else None,
                            limits.document_max_bytes,
                        )
                        change = (
                            self._upsert(file.root, rendered)
                            if rendered.sections
                            else Delete(root_id=file.root, source_revision=rendered.revision)
                        )
                        if isinstance(change, Upsert):
                            change = replace(
                                change, source_revision=change.source_revision + ":" + stamp
                            )
                    except render.InvalidDocument:
                        change = self._bad(file.root, repo, "github-file", file.blob)
                    acquired.append(change)
                    if len(acquired) == 8:
                        repo.verify(self.client)
                        for prepared in acquired:
                            yield from hand(prepared)
                        acquired.clear()
                if acquired:
                    repo.verify(self.client)
                    for prepared in acquired:
                        yield from hand(prepared)
                if (
                    self.tree.head(repo, item) != commit
                    or Repository.read(self.client, repo.id) != repo
                ):
                    raise ValueError("github_branch_moved_during_read")
                for root in list(self.known):
                    if root.startswith(f"github:repo:{item.id}:file:") and root not in current:
                        yield from hand(Delete(root_id=root, source_revision=commit))
            finished = int(self.client.server_time.timestamp())
            state.repos[item.id] = RepoCursor(
                frontier,
                commit,
                next_rotate,
                finished,
                finished
                if completed
                else (previous.rotation_start if previous.rotation_start is not None else now),
            )
            state.observed.add(item.id)
        yield flush(state.encode())

    def _ids(self, repo: Repository, item: RepositoryScope):
        for ref in conversations(self.client, repo, item, None):
            # Canonical binding is checked even when no body is needed.
            try:
                self._parent(repo, ref)
            except ApiError as error:
                if error.status == 404 or error.moved:
                    continue
                raise
            yield ref.root
        if item.prose or item.code:
            commit = self.tree.head(repo, item)
            self._listing_heads[item.id] = commit
            yield from (file.root for file in self.tree.files(repo, item, commit))
            if self.tree.head(repo, item) != commit:
                raise ValueError("github_branch_moved_during_listing")

    def _listing_ids(self, observed: set[int] | None = None) -> set[str]:
        self._missing_roots(observed or set())
        roots: set[str] = set()
        self._listing_heads: dict[int, str] = {}
        for item in self.scope.repositories:
            if item.id not in self.repos:
                continue
            repo = self.repos[item.id]
            roots.update(self._ids(repo, item))
            # Offset pagination can miss an old object as updates reorder it.
            for root in self.known:
                ref = ConversationRef.decode(root)
                if ref is None or ref.repo_id != item.id or root in roots:
                    continue
                if not (
                    (ref.kind == FeedKind.ISSUE and item.issues)
                    or (ref.kind == FeedKind.PR and item.pull_requests)
                ):
                    continue
                try:
                    self._parent(repo, ref)
                except ApiError as error:
                    if error.status == 404 or error.moved:
                        continue
                    raise
                roots.add(root)
            current = Repository.read(self.client, item.id)
            if current != repo:
                raise ValueError("github_repository_moved_during_listing")
            if len(roots) > MAX_ENTRIES:
                raise ValueError("github_listing_limit")
        for item in self.scope.repositories:
            # Check earlier repositories again after the later ones were read.
            if (
                item.id in self.repos
                and (item.prose or item.code)
                and self.tree.head(self.repos[item.id], item) != self._listing_heads[item.id]
            ):
                raise ValueError("github_branch_moved_during_listing")
        if inventory(self.client) != self.installed:
            raise ValueError("github_inventory_changed_during_listing")
        return roots

    def listing(self, cursor: str | None) -> Iterator[ListingBatch]:
        self._open()
        roots = sorted(self._listing_ids())
        listing_id = hashlib.sha256(f"{cursor}:{time.time_ns()}".encode()).hexdigest()
        width = min(256, self.context.limits.max_rows)
        parts = [roots[start : start + width] for start in range(0, len(roots), width)] or [[]]
        for part, ids in enumerate(parts):
            last = part + 1 == len(parts)
            yield ListingBatch(
                listing_id=listing_id,
                part=part,
                last=last,
                complete=last and self.context.next_revisions is not None,
                root_ids=ids,
                cursor=f"{listing_id}:{part}",
            )

    def permissions(self, cursor: str | None) -> Iterator[PermissionBatch]:
        if self.context.permissions == "fixed":
            return
        self._open()
        state = Cursor.decode(cursor, self.scope)
        self._missing_roots(state.observed)
        # Every required inventory, catalog, negative check and ACL read must
        # finish before the first permission batch can renew source freshness.
        roots = self._listing_ids(state.observed) | set(self.known)
        access = AccessSnapshot.read(self.client, self.repos)
        configured = {repo.id for repo in self.scope.repositories}
        repository_ids = configured | {int(root.split(":")[2]) for root in roots}
        grants = []
        for repo_id in sorted(repository_ids):
            readers = access.readers.get(repo_id, ())
            revision = hashlib.sha256(json.dumps(readers).encode()).hexdigest()
            grants.append(
                ContainerGrant(
                    container_id=container(repo_id),
                    container_kind="github_repository",
                    permission_revision=revision,
                    readers=readers,
                )
            )
        documents = [
            DocumentPermission(
                root_id=root,
                permission_revision=hashlib.sha256(("bind:" + root).encode()).hexdigest(),
                readers=(),
                containers=(container(int(root.split(":")[2])),),
            )
            for root in sorted(roots)
        ]
        state.observed.update(self.repos)
        frontier = state.encode()
        batches: list[PermissionBatch] = []
        # Seal all frames before yielding, including their serialization bounds.
        # No provider call follows the first yield.
        from ..runtime import encode

        width = min(256, self.context.limits.max_rows)

        def seal[EntryT](
            collection: Sequence[EntryT], make: Callable[[Sequence[EntryT]], PermissionBatch]
        ) -> None:
            pending: list[EntryT] = []
            for entry in collection:
                candidate = make([*pending, entry])
                header, _ = encode(self.context.source_id, SyncKind.PERMISSIONS, cursor, candidate)
                if (
                    len(json.dumps(header).encode()) > self.context.limits.max_bytes
                    or len(pending) >= width
                ):
                    if not pending:
                        raise ValueError("github_permission_batch_limit")
                    batches.append(make(pending))
                    pending = []
                    candidate = make([entry])
                    header, _ = encode(
                        self.context.source_id, SyncKind.PERMISSIONS, cursor, candidate
                    )
                    if len(json.dumps(header).encode()) > self.context.limits.max_bytes:
                        raise ValueError("github_permission_batch_limit")
                pending.append(entry)
            if pending:
                batches.append(make(pending))

        seal(grants, lambda rows: PermissionBatch(container_grants=rows, cursor=frontier))
        seal(documents, lambda rows: PermissionBatch(documents=rows, cursor=frontier))
        yield from batches or [PermissionBatch(cursor=frontier)]

    def members(self, cursor: str | None) -> Iterator[PrincipalBatch]:
        if self.context.permissions == "fixed":
            return
        self._open()
        state = Cursor.decode(cursor, self.scope)
        self._missing_roots(state.observed)
        access = AccessSnapshot.read(self.client, self.repos)
        state.observed.update(self.repos)
        frontier = state.encode()
        snapshot_id = hashlib.sha256(f"{frontier}:{time.time_ns()}".encode()).hexdigest()
        width = min(256, self.context.limits.max_rows)
        pages = [
            access.users[start : start + width] for start in range(0, len(access.users), width)
        ] or [()]
        batches = [
            PrincipalBatch(
                snapshot_id=snapshot_id,
                part=part,
                last=part + 1 == len(pages),
                users=users,
                cursor=frontier,
            )
            for part, users in enumerate(pages)
        ]
        from ..runtime import encode

        for batch in batches:
            header, _ = encode(self.context.source_id, SyncKind.MEMBERS, cursor, batch)
            if len(json.dumps(header).encode()) > self.context.limits.max_bytes:
                raise ValueError("github_member_batch_limit")
        yield from batches
