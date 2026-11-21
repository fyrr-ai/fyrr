"""Four independent Jira feeds; safe progress comes after acknowledged batches."""

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime

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
from ..runtime import encode
from ..validate import ContractViolation
from . import render
from .adf import ADFError
from .auth import Identity
from .client import MAX_ENTRIES, ApiError, Client, number
from .comments import Comment, visibility_signature
from .config import Scope, Settings
from .feeds import Cursor, IssueBinding, fingerprint, rotation, stamp
from .members import Directory, account_id, user_id
from .permissions import DYNAMIC, PolicyGraph, container, digest, field_id, policy_token
from .projects import Project


class Jira(Connector[Settings, Scope]):
    Settings = Settings
    Scope = Scope

    def _open(self, cursor=None):
        self.settings, self.scope = self.context.settings, self.context.scope
        if self.context.permissions == "fixed" and not self.context.readers:
            raise ValueError("jira_fixed_readers_required")
        key = hashlib.sha256(
            (
                fingerprint(self.settings, self.scope)
                + json.dumps(
                    [
                        self.context.permissions,
                        self.context.readers if self.context.permissions == "fixed" else (),
                    ]
                )
            ).encode()
        ).hexdigest()
        self.state = Cursor.decode(cursor, key)
        self.client = Client(self.settings)
        self.identity = Identity.read(self.client, self.settings)
        self.known: dict[str, str] = {}
        self.projects = {}
        self.gone = set()
        self.graph = None
        if self.context.next_revisions:
            while page := self.context.next_revisions():
                self.known.update(page)
                if len(self.known) > MAX_ENTRIES:
                    raise ValueError("jira_revision_view_limit")
        self.ids = {
            self._id(key): value for key, value in self.known.items() if self._id(key) is not None
        }
        for identity in self.scope.projects:
            self._project(
                identity,
                known=any(
                    (b := IssueBinding.from_revision(v)) and b.project == identity
                    for v in self.ids.values()
                ),
            )

    def _root(self, identity):
        return f"jira:{self.identity.cloud}:issue:{identity}"

    def _id(self, key):
        prefix = f"jira:{self.identity.cloud}:issue:"
        if key.startswith(prefix):
            try:
                return number(key[len(prefix) :])
            except ValueError:
                return None
        return None

    def _project(self, identity, known=False):
        try:
            project = Project.read(self.client, identity)
        except ApiError as error:
            if not error.gone or not known:
                raise
            self.gone.add(identity)
            return None
        if project.archived:
            self.gone.add(identity)
            return None
        if self.context.permissions == "sync" and (
            project.simplified or project.kind not in ("software", "business")
        ):
            raise ValueError("jira_sync_company_managed_only")
        self.projects[identity] = project
        return project

    def _fetch(self, identity, queued_project=None):
        try:
            issue = self.client.call(f"/issue/{identity}")
        except ApiError as error:
            if not error.gone or identity not in self.ids:
                raise
            return None
        if (
            not isinstance(issue, dict)
            or number(issue.get("id")) != identity
            or not isinstance(issue.get("fields"), dict)
        ):
            raise ValueError("jira_issue_identity_mismatch")
        project_id = number(issue["fields"].get("project", {}).get("id"))
        if queued_project is not None and project_id != queued_project:
            raise ValueError("jira_issue_project_changed")
        if (
            not isinstance(issue.get("key"), str)
            or not issue["key"]
            or len(issue["key"].encode()) > 256
        ):
            raise ValueError("jira_issue_key_invalid")
        if not self.scope.matches(issue):
            return None
        if self.scope.jql:
            matches = list(
                self.client.search(self._query(extra=f"id = {identity}"), reconcile=(identity,))
            )
            if not matches:
                # A negative enhanced-search answer must surround a stable canonical read.
                try:
                    after = self.client.call(f"/issue/{identity}")
                except ApiError as error:
                    if error.gone:
                        return None
                    raise
                if after != issue:
                    raise ValueError("jira_scope_changed_during_read")
                return None
            if (
                len(matches) != 1
                or number(matches[0]["id"]) != identity
                or number(matches[0]["fields"]["project"]["id"]) != project_id
                or matches[0]["fields"]["updated"] != issue["fields"]["updated"]
            ):
                raise ValueError("jira_scope_membership_unproven")
        project = self._project(project_id, known=identity in self.ids)
        if project is None:
            return None
        return issue

    def _canonical(self, identity, queued_project=None):
        before = self._fetch(identity, queued_project)
        if before is None:
            return None, ()
        comments = Comment.read(self.client, identity, self.scope.skip_comment_authors)
        after = self._fetch(identity, number(before["fields"]["project"]["id"]))
        if after != before:
            raise ValueError("jira_issue_changed_during_read")
        return after, comments

    def _query(self, lower=0, upper=None, extra=""):
        query = "(" + self.scope.query() + ")"
        if self.scope.issue_types:
            query += " AND issuetype in (" + ",".join(map(str, self.scope.issue_types)) + ")"
        if self.scope.labels:
            query += " AND labels in (" + ",".join(json.dumps(v) for v in self.scope.labels) + ")"
        # Explicit UTC literals; overlap accounts for Jira JQL minute precision.
        if lower:
            query += (
                ' AND updated >= "'
                + datetime.fromtimestamp(max(0, lower - 120), UTC).strftime("%Y-%m-%d %H:%M")
                + '"'
            )
        if upper is not None:
            query += (
                ' AND updated <= "'
                + datetime.fromtimestamp(upper + 60, UTC).strftime("%Y-%m-%d %H:%M")
                + '"'
            )
        if extra:
            query += " AND " + extra
        return query

    def _candidates(self, lower=0, upper=None, extra=""):
        return {
            number(item["id"]): item
            for item in self.client.search(self._query(lower, upper, extra))
        }

    def _change(self, issue, comments):
        identity = number(issue["id"])
        project = self.projects[number(issue["fields"]["project"]["id"])]
        targets: tuple[tuple, ...] = ()
        dynamic = False
        groups = ()
        if self.context.permissions == "sync":
            targets, dynamic, groups = self._content_binding(issue, comments)
        try:
            revision, meta, sections = render.document(
                self.identity, project, issue, comments, self.context, targets, dynamic, groups
            )
            change = Upsert(
                root_id=self._root(identity),
                source_revision=revision,
                metadata=meta,
                sections=sections,
                attributes=("jira.key=" + issue["key"],),
                readers=self.context.readers if self.context.permissions == "fixed" else (),
            )
            encoded = document_changes([change])
        except (ADFError, ContractViolation):
            revision = IssueBinding(
                project.id,
                render.number_level(issue["fields"]),
                dynamic,
                tuple(sorted({tuple(t[1:]) for t in targets})),
                groups,
                visibility_signature(comments) if self.context.permissions == "sync" else None,
            ).revision(hashlib.sha256(("bad:" + self._root(identity)).encode()).hexdigest())
            meta = Metadata(
                source_uri=self.identity.site + "/browse/" + issue["key"], source_type="jira-issue"
            )
            sections = (Section(text="\0"),)
            change = Upsert(
                root_id=self._root(identity),
                source_revision=revision,
                metadata=meta,
                sections=sections,
                readers=self.context.readers if self.context.permissions == "fixed" else (),
            )
            encoded = document_changes([change])
        if encoded.get_total_buffer_size() > self.context.limits.max_bytes:
            change = replace(
                change,
                source_revision=IssueBinding(project.id).revision(
                    hashlib.sha256(("envelope_bad:" + revision).encode()).hexdigest()
                ),
                sections=(Section(text="\0"),),
                metadata=Metadata(source_uri=self.identity.site, source_type="jira-issue"),
            )
        if document_changes([change]).get_total_buffer_size() > self.context.limits.max_bytes:
            raise ValueError("jira_batch_limit")
        return change

    def content(self, cursor):
        self._open(cursor)
        durable = cursor or Cursor(self.state.scope).encode()
        upper = self.client.now
        rotated, marker, complete = rotation(
            self.ids, self.state.marker, upper - self.state.step if self.state.step else 900
        )
        full = not self.state.full or upper - self.state.full >= 43200
        lower = 0 if full else self.state.updated
        candidates = self._candidates(lower, upper)
        for identity in rotated:
            candidates.setdefault(identity, None)
        observed = {}
        width = min(100, self.context.limits.max_rows)
        for _ in range(3):
            identities = sorted(candidates)
            for offset in range(0, len(identities), width):
                chunk = identities[offset : offset + width]
                hints = {identity: candidates[identity] for identity in chunk}
                readings = {}
                for _ in range(3):
                    for identity, hint in hints.items():
                        issue, comments = self._canonical(
                            identity, number(hint["fields"]["project"]["id"]) if hint else None
                        )
                        readings[identity] = (issue, comments)
                        if issue or hint:
                            updated = (issue or hint)["fields"]["updated"]
                            stamp(updated)
                            observed[identity] = updated
                    catchup = self._candidates(
                        lower,
                        self.client.now,
                        extra="id in (" + ",".join(map(str, chunk)) + ")",
                    )
                    hints = {
                        identity: item
                        for identity, item in catchup.items()
                        if observed.get(identity) != item["fields"]["updated"]
                    }
                    if not hints:
                        break
                else:
                    raise ValueError("jira_discovery_did_not_converge")
                pending: list[Upsert | Delete] = []
                for identity, (issue, comments) in readings.items():
                    change = (
                        self._change(issue, comments)
                        if issue
                        else Delete(root_id=self._root(identity), source_revision="out_of_scope")
                    )
                    if (
                        isinstance(change, Upsert)
                        and self.known.get(change.root_id) == change.source_revision
                    ):
                        continue
                    candidate = [*pending, change]
                    if (
                        pending
                        and document_changes(candidate).get_total_buffer_size()
                        > self.context.limits.max_bytes
                    ):
                        yield ContentBatch(changes=document_changes(pending), cursor=durable)
                        pending = []
                    pending.append(change)
                if pending:
                    yield ContentBatch(changes=document_changes(pending), cursor=durable)
            catchup = self._candidates(lower, self.client.now)
            candidates = {
                identity: item
                for identity, item in catchup.items()
                if observed.get(identity) != item["fields"]["updated"]
            }
            if not candidates:
                break
        else:
            raise ValueError("jira_discovery_did_not_converge")
        if full:
            self.state.full = upper
        self.state.updated = upper
        self.state.marker = marker
        self.state.step = self.client.now
        if complete:
            self.state.start = self.client.now
        elif not self.state.start:
            self.state.start = upper
        yield ContentBatch(changes=document_changes([]), cursor=self.state.encode())

    def listing(self, cursor):
        self._open()
        previous = None
        roots = set()
        for _ in range(3):
            census = self._candidates(upper=self.client.now)
            bindings = {}
            for identity, hint in census.items():
                issue = self._fetch(identity, number(hint["fields"]["project"]["id"]))
                if issue:
                    bindings[identity] = (
                        number(issue["fields"]["project"]["id"]),
                        render.number_level(issue["fields"]),
                    )
            # Independently check every missing acknowledged/pending root.
            for identity in self.ids.keys() - bindings.keys():
                issue = self._fetch(identity)
                if issue:
                    match = list(
                        self.client.search(
                            self._query(extra=f"id = {identity}"), reconcile=(identity,)
                        )
                    )
                    if match:
                        bindings[identity] = (
                            number(issue["fields"]["project"]["id"]),
                            render.number_level(issue["fields"]),
                        )
                    elif not self.scope.projects:
                        # Enhanced search reconciles this specific ID; verify the
                        # canonical binding did not change around its negative answer.
                        if self._fetch(identity) != issue:
                            raise ValueError("jira_listing_membership_unproven")
                    else:
                        raise ValueError("jira_listing_search_lag")
            if bindings == previous:
                roots = {self._root(identity) for identity in bindings}
                break
            previous = bindings
        else:
            raise ValueError("jira_listing_did_not_converge")
        listing_id = hashlib.sha256(json.dumps(sorted(roots)).encode()).hexdigest()
        width = max(1, self.context.limits.max_rows)
        parts = [sorted(roots)[i : i + width] for i in range(0, len(roots), width)] or [[]]
        for part, values in enumerate(parts):
            last = part == len(parts) - 1
            yield ListingBatch(
                listing_id=listing_id,
                part=part,
                last=last,
                complete=last and self.context.next_revisions is not None,
                root_ids=values,
                cursor=f"{listing_id}:{part}",
            )

    def _graph(self):
        if self.graph is None:
            self.graph = PolicyGraph(self.client, self.settings, Directory.read(self.client))
        return self.graph

    def _content_binding(self, issue, comments):
        graph = self._graph()
        project = number(issue["fields"]["project"]["id"])
        graph.project(project)
        level = render.number_level(issue["fields"])
        holders = graph.rules[project] + graph.levels[project].get(level, [])
        dynamic = False
        groups: set[str] = set()
        for holder in holders:
            kind = holder["type"]
            if kind not in DYNAMIC:
                continue
            key = kind if kind in ("reporter", "assignee") else field_id(holder)
            body = issue["fields"].get(key)
            if body:
                dynamic = True
            if kind == "groupCF" and body:
                entries = body if isinstance(body, list) else [body]
                for entry in entries:
                    group = account_id(entry.get("groupId") if isinstance(entry, dict) else None)
                    graph.group(group)
                    groups.add(group)
        targets = []
        for comment in comments:
            if not comment.restricted:
                continue
            target = graph.target(project, comment.body["visibility"])
            if target:
                graph.audience(project, level, target)
                targets.append((comment.id, *target))
        return tuple(targets), dynamic, tuple(sorted(groups))

    def _access(self, cursor):
        self._open(cursor)
        graph = self._graph()
        descriptors = {
            identity: IssueBinding.from_revision(revision)
            for identity, revision in self.ids.items()
        }
        projects = set(self.scope.projects) | {b.project for b in descriptors.values() if b}
        if self.scope.projects:
            self.gone.update(projects - set(self.scope.projects))
        for project in sorted(projects):
            if project in self.gone:
                continue
            current = self._project(
                project, known=any(b and b.project == project for b in descriptors.values())
            )
            if current is None:
                continue
            graph.project(project)
            graph.audience(project)
        for binding in descriptors.values():
            if not binding or binding.project in self.gone:
                continue
            for group in binding.groups:
                graph.group(group)
            graph.audience(binding.project, binding.level)
            for target in binding.targets:
                graph.audience(binding.project, binding.level, target)
        before_hash = graph.membership_hash()
        holder_types = graph.holder_type_hash()
        upper = self.client.now
        lower = 0 if not self.state.full or upper - self.state.full >= 43200 else self.state.updated
        candidates = self._candidates(lower, upper)
        rotated, marker, complete = rotation(
            self.ids, self.state.marker, upper - self.state.step if self.state.step else 900
        )
        for identity in rotated:
            candidates.setdefault(identity, None)
        # A policy change must revoke direct users now, not on the next rotation.
        if self.state.policies != before_hash or self.state.holder_types != holder_types:
            for project in sorted(projects - self.gone):
                graph.project(project)
                if self.state.holder_types != holder_types:
                    candidates.update(self._candidates(extra=f"project = {project}"))
                    continue
                levels = set(graph.levels[project]) | {
                    b.level
                    for b in descriptors.values()
                    if b and b.project == project and b.level is not None
                }
                for level in levels:
                    candidates.update(
                        self._candidates(extra=f"project = {project} AND level = {level}")
                    )
                # Previously dynamic bindings also need a record when their holder disappears.
                old = [
                    identity
                    for identity, b in descriptors.items()
                    if b and b.project == project and b.dynamic and b.level is None
                ]
                for offset in range(0, len(old), 100):
                    values = old[offset : offset + 100]
                    candidates.update(
                        self._candidates(
                            extra="project = "
                            + str(project)
                            + " AND id in ("
                            + ",".join(map(str, values))
                            + ")"
                        )
                    )
                terms = []
                for holder in graph.rules[project]:
                    kind = holder["type"]
                    if kind in ("reporter", "assignee"):
                        terms.append(kind + " IS NOT EMPTY")
                    elif kind in ("userCF", "groupCF"):
                        value = field_id(holder)
                        terms.append("cf[" + value[12:] + "] IS NOT EMPTY")
                if terms:
                    candidates.update(
                        self._candidates(
                            extra=f"project = {project} AND level IS EMPTY AND ("
                            + " OR ".join(terms)
                            + ")"
                        )
                    )
        observed = {}
        documents = {}
        for _ in range(3):
            for identity, hint in candidates.items():
                issue, comments = self._canonical(
                    identity, number(hint["fields"]["project"]["id"]) if hint else None
                )
                if issue:
                    binding = descriptors.get(identity)
                    if identity in self.ids and (
                        binding is None
                        or (
                            binding.visibility is not None
                            and binding.visibility != visibility_signature(comments)
                        )
                        or (
                            binding.visibility is None
                            and (binding.targets or any(c.restricted for c in comments))
                        )
                    ):
                        raise ValueError("jira_published_visibility_stale")
                    targets, dynamic, groups = self._content_binding(issue, comments)
                    project = number(issue["fields"]["project"]["id"])
                    level = render.number_level(issue["fields"])
                    graph.audience(project, level)
                    tokens = {policy_token(self.identity.cloud, project, level)}
                    tokens.update(
                        policy_token(self.identity.cloud, project, level, tuple(t[1:]))
                        for t in targets
                    )
                    tokens.update(
                        "usr:" + user_id(self.identity.cloud, u) for u in graph.issue_readers(issue)
                    )
                    containers: tuple[str, ...] = ()
                    if level is None and not dynamic:
                        tokens.discard(policy_token(self.identity.cloud, project))
                        containers = (container(self.identity.cloud, project),)
                    readers = tuple(sorted(tokens))
                    revision = digest([project, level, readers, containers])
                    documents[identity] = DocumentPermission(
                        root_id=self._root(identity),
                        permission_revision=revision,
                        readers=readers,
                        containers=containers,
                    )
                    stamp(issue["fields"]["updated"])
                    observed[identity] = issue["fields"]["updated"]
                else:
                    documents[identity] = DocumentPermission(
                        root_id=self._root(identity),
                        permission_revision=digest(["gone"]),
                        readers=(),
                        containers=(),
                    )
                    if hint:
                        stamp(hint["fields"]["updated"])
                        observed[identity] = hint["fields"]["updated"]
            catchup = self._candidates(lower, self.client.now)
            late = {
                identity: item
                for identity, item in catchup.items()
                if identity not in observed or observed[identity] != item["fields"]["updated"]
            }
            if not late:
                break
            candidates = late
        else:
            raise ValueError("jira_permission_discovery_did_not_converge")
        for identity, binding in descriptors.items():
            if binding and binding.project in self.gone:
                documents[identity] = DocumentPermission(
                    root_id=self._root(identity),
                    permission_revision=digest(["gone"]),
                    readers=(),
                    containers=(),
                )
        groups = graph.finish()
        grants = []
        for project in sorted(projects | set(graph.rules)):
            readers = () if project in self.gone else (policy_token(self.identity.cloud, project),)
            grants.append(
                ContainerGrant(
                    container_id=container(self.identity.cloud, project),
                    container_kind="jira_project",
                    permission_revision=digest(readers),
                    readers=readers,
                )
            )
        self.state.updated = upper
        if lower == 0:
            self.state.full = upper
        self.state.marker = marker
        self.state.step = self.client.now
        if complete or not self.state.start:
            self.state.start = self.client.now
        self.state.policies = graph.membership_hash()
        self.state.holder_types = holder_types
        return (
            tuple(documents.values()),
            tuple(grants),
            groups,
            graph.directory.users(self.settings),
        )

    def _preflight(self, kind, cursor, batches):
        for batch in batches:
            header, _ = encode(self.context.source_id, kind, cursor, batch)
            if len(json.dumps(header).encode()) > self.context.limits.max_bytes:
                raise ValueError("jira_snapshot_envelope_limit")
            cursor = batch.cursor

    def permissions(self, cursor):
        if self.context.permissions == "fixed":
            return
        documents, grants, _, _ = self._access(cursor)
        safe = cursor or Cursor(self.state.scope).encode()
        doc_width = min(100, self.context.limits.max_rows)
        grant_width = min(64, self.context.limits.max_rows)
        batches = [
            PermissionBatch(documents=documents[i : i + doc_width], cursor=safe)
            for i in range(0, len(documents), doc_width)
        ]
        batches += [
            PermissionBatch(container_grants=grants[i : i + grant_width], cursor=safe)
            for i in range(0, len(grants), grant_width)
        ]
        if not batches:
            batches = [PermissionBatch(cursor=safe)]
        batches[-1] = replace(batches[-1], cursor=self.state.encode())
        self._preflight(SyncKind.PERMISSIONS, cursor, batches)
        yield from batches

    def members(self, cursor):
        if self.context.permissions == "fixed":
            return
        _, _, groups, users = self._access(cursor)
        snapshot = digest(
            [self.state.encode(), [g.__dict__ for g in groups], [u.__dict__ for u in users]]
        )
        user_width = min(500, self.context.limits.max_rows)
        group_width = min(100, self.context.limits.max_rows)
        pages = [
            PrincipalBatch(
                snapshot_id=snapshot,
                part=0,
                last=False,
                users=users[i : i + user_width],
                cursor=cursor or self.state.encode(),
            )
            for i in range(0, len(users), user_width)
        ]
        pages += [
            PrincipalBatch(
                snapshot_id=snapshot,
                part=0,
                last=False,
                groups=groups[i : i + group_width],
                cursor=cursor or self.state.encode(),
            )
            for i in range(0, len(groups), group_width)
        ]
        if not pages:
            pages = [
                PrincipalBatch(snapshot_id=snapshot, part=0, last=True, cursor=self.state.encode())
            ]
        batches = [
            replace(
                batch,
                part=i,
                last=i == len(pages) - 1,
                cursor=self.state.encode() if i == len(pages) - 1 else batch.cursor,
            )
            for i, batch in enumerate(pages)
        ]
        self._preflight(SyncKind.MEMBERS, cursor, batches)
        yield from batches
