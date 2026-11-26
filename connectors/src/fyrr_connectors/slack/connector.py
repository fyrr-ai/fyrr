"""Scoped Slack acquisition, acknowledgment and deletion-safe reconciliation."""

import hashlib
import json
import re
import time
from bisect import bisect_left, bisect_right
from dataclasses import replace

from .._http import MAX_ENTRIES
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
from ..runtime import header_bytes
from . import render, threads
from .auth import Identity
from .client import ApiError, Client
from .config import Scope, Settings
from .feeds import Channel, ChannelCursor, Cursor, catalog, history, rotation
from .permissions import AccessSnapshot, container
from .windows import Message, WindowAnchor, build


def root(workspace, channel, kind, stamp):
    return f"slack:{workspace}:{channel}:{kind}:{stamp}"


def decode(value, workspace):
    match = re.fullmatch(
        r"slack:(T[A-Z0-9]{2,31}):([CG][A-Z0-9]{2,31}):(window|thread):([0-9]{1,18})", value
    )
    if match and match[1] == workspace:
        return match[2], match[3], int(match[4])
    return None


class Slack(Connector[Settings, Scope]):
    Settings = Settings
    Scope = Scope

    def _open(self, cursor=None):
        self.settings, self.scope = self.context.settings, self.context.scope
        assert isinstance(self.settings, Settings) and isinstance(self.scope, Scope)
        if self.scope.include_shared and self.context.permissions != "fixed":
            raise ValueError("slack_shared_requires_fixed")
        self.state = Cursor.decode(
            cursor, self.scope, self.settings.window_gap_seconds, self.settings.workspace_id
        )
        self.known: dict[str, str] = {}
        if self.context.next_revisions:
            while page := self.context.next_revisions():
                self.known.update(page)
                if len(self.known) > MAX_ENTRIES:
                    raise ValueError("slack_revision_view_limit")
        self.known_ids = {
            parts[0] for key in self.known if (parts := decode(key, self.settings.workspace_id))
        } | set(self.state.channels)
        self.client = Client(self.settings.api_endpoint, self.settings.token.reveal())
        self.identity = Identity.read(self.client, self.settings)
        self.channels, self.gone = catalog(self.client, self.scope, self.known_ids)
        allocation = min(8, 64 // max(1, len(self.channels)))
        for channel in self.channels.values():
            if channel.id not in self.state.channels:
                seeds = []
                for stamp in self._refs(channel, "window")[-allocation:]:
                    revision = self.known[
                        root(self.identity.workspace, channel.id, "window", stamp)
                    ]
                    match = re.fullmatch(r"[0-9a-f]{64}:([0-9]{1,18})", revision)
                    if match and stamp <= int(match[1]) < 10**18:
                        seeds.append([stamp, int(match[1])])
                self.state.channels[channel.id] = ChannelCursor(anchors=seeds)
        # A frontier can skip history only while this channel has live roots.
        self.state.channels = {
            key: value
            for key, value in self.state.channels.items()
            if key in self.channels
            and key
            in {
                parts[0]
                for root_id in self.known
                if (parts := decode(root_id, self.identity.workspace))
            }
        }
        self.migrations = {}
        self.names = {}
        self.labels = {key: item.name for key, item in self.channels.items()}

    def _name(self, user):
        if user is None:
            return None
        if not isinstance(user, str) or not re.fullmatch(r"[UW][A-Z0-9]{2,31}", user):
            raise render.InvalidDocument
        if user not in self.names:
            if len(self.names) >= MAX_ENTRIES:
                raise ValueError("slack_name_cache_limit")
            try:
                body = self.client.get("users.info", user=user).get("user")
            except ApiError as error:
                if error.code != "user_not_found":
                    raise
                body = {}
            if not isinstance(body, dict) or body.get("id", user) != user:
                raise ValueError("slack_user_identity_invalid")
            profile = body.get("profile", {})
            if not isinstance(profile, dict):
                raise render.InvalidDocument
            self.names[user] = render.display(profile.get("display_name") or body.get("real_name"))
        return self.names[user]

    def _refs(self, channel, kind):
        return sorted(
            {
                parts[2]
                for key in self.known
                if (parts := decode(key, self.identity.workspace))
                and parts[:2] == (channel.id, kind)
            }
        )

    def _messages(self, payloads, known_threads):
        result = []
        for body in payloads.values():
            item = Message.parse(body, self.scope.bots)
            if item is not None:
                if item.eligible:
                    try:
                        item = replace(
                            item, body=render.mentions(item.body, self._name, self.labels)
                        )
                    except render.InvalidDocument:
                        item = replace(item, body="\0")
                if item.ts in known_threads:
                    item = replace(item, thread=item.ts)
                result.append(item)
        return result

    def _change(self, channel, kind, stamp, messages):
        identity = root(self.identity.workspace, channel.id, kind, stamp)
        try:
            rendered = render.document(
                self.identity,
                channel,
                kind,
                stamp,
                messages,
                self._name,
                self.labels,
                self.context.limits.document_max_bytes,
            )
        except render.InvalidDocument:
            return Upsert(
                root_id=identity,
                source_revision=hashlib.sha256(
                    f"bad:{render.FORMAT}:{identity}".encode()
                ).hexdigest(),
                metadata=Metadata(
                    source_uri=f"{self.identity.url}/archives/{channel.id}",
                    source_type="slack-" + kind,
                ),
                sections=(Section(text="\0"),),
                readers=self.context.readers if self.context.permissions == "fixed" else (),
            )
        if rendered is None:
            return Delete(root_id=identity, source_revision="empty")
        revision = (
            rendered.revision + ":" + str(max(item.ts for item in messages))
            if kind == "window"
            else rendered.revision
        )
        return Upsert(
            root_id=identity,
            source_revision=revision,
            metadata=rendered.metadata,
            sections=rendered.sections,
            readers=self.context.readers if self.context.permissions == "fixed" else (),
        )

    def _content_channel(self, channel, previous):
        frontier = self.client.now
        known_windows, known_threads = self._refs(channel, "window"), self._refs(channel, "thread")
        positions = {stamp: index for index, stamp in enumerate(known_windows)}
        elapsed = max(1, frontier - previous.step) if previous.step else 900_000_000
        selected, wmarker, wcomplete = rotation(known_windows, previous.windows, elapsed)
        thread_scan, tmarker, tcomplete = rotation(known_threads, previous.threads, elapsed)
        anchors = [WindowAnchor(*pair) for pair in previous.anchors]
        tail = max((pair[0] for pair in previous.anchors), default=previous.since)
        oldest = min(tail, max(0, previous.since - 3600_000_000)) if previous.since else 0
        payloads = history(self.client, channel, oldest, frontier)
        for stamp in selected:
            position = positions[stamp]
            end = known_windows[position + 1] - 1 if position + 1 < len(known_windows) else frontier
            payloads.update(history(self.client, channel, stamp, end))
        messages = self._messages(payloads, known_threads)
        windows = build(messages, anchors, self.settings.window_gap_seconds)
        changes = [self._change(channel, "window", item.start, item.messages) for item in windows]
        # Tracked empty windows can be deleted authoritatively. Beyond the ledger
        # cap, a lost first timestamp is re-keyed and its orphan waits for listing.
        scanned = [(oldest, frontier)] + [
            (
                stamp,
                known_windows[positions[stamp] + 1] - 1
                if positions[stamp] + 1 < len(known_windows)
                else frontier,
            )
            for stamp in selected
        ]
        for anchor in anchors:
            if any(
                start <= anchor.start and anchor.end <= end for start, end in scanned
            ) and not any(
                anchor.start <= item.ts <= anchor.end
                for item in messages
                if item.eligible and item.thread is None
            ):
                changes.append(
                    Delete(
                        root_id=root(self.identity.workspace, channel.id, "window", anchor.start),
                        source_revision="empty",
                    )
                )
        discovered = {item.thread for item in messages if item.thread is not None}
        for anchor in anchors:
            targets = {
                root(self.identity.workspace, channel.id, "thread", item.thread)
                for item in messages
                if item.thread is not None and anchor.start <= item.ts <= anchor.end
            }
            if targets:
                self.migrations[
                    root(self.identity.workspace, channel.id, "window", anchor.start)
                ] = targets
        thread_changes = []
        for stamp in sorted(discovered | set(thread_scan)):
            thread = threads.read(
                self.client, channel, stamp, self.scope.bots, self.context.limits.document_max_bytes
            )
            thread_changes.append(
                self._change(channel, "thread", stamp, thread.messages)
                if thread
                else Delete(
                    root_id=root(self.identity.workspace, channel.id, "thread", stamp),
                    source_revision="empty",
                )
            )
        if Channel.read(self.client, channel.id) != channel:
            raise ValueError("slack_channel_changed_during_read")
        ledger = {pair[0]: pair for pair in previous.anchors}
        for item in windows:
            ledger[item.start] = [item.start, item.end]
        return thread_changes + changes, ChannelCursor(
            frontier,
            wmarker,
            tmarker,
            self.client.now,
            self.client.now if wcomplete and tcomplete else (previous.start or frontier),
            [ledger[key] for key in sorted(ledger)],
        )

    def content(self, cursor):
        self._open(cursor)
        durable = cursor or Cursor(self.state.scope).encode()
        pending: dict[str, Upsert | Delete] = {}
        acknowledged = set(self.known)

        def flush(end):
            batch = ContentBatch(changes=document_changes(pending.values()), cursor=end)
            pending.clear()
            return batch

        for identity, channel in list(self.channels.items()):
            try:
                changes, state = self._content_channel(
                    channel, self.state.channels.get(identity, ChannelCursor())
                )
            except ApiError as error:
                if not error.gone or identity not in self.known_ids:
                    raise
                self.gone.add(identity)
                self.channels.pop(identity)
                continue
            bad = {
                change.root_id
                for change in changes
                if isinstance(change, Upsert)
                and any(section.text == "\0" for section in change.sections)
            }
            for change in changes:
                if (
                    isinstance(change, Upsert)
                    and self.known.get(change.root_id) == change.source_revision
                ):
                    continue
                if (
                    document_changes([change]).get_total_buffer_size()
                    > self.context.limits.max_bytes
                    or document_changes([change]).num_rows > self.context.limits.max_rows
                ) and isinstance(change, Upsert):
                    change = replace(
                        change,
                        source_revision=hashlib.sha256(
                            ("envelope_bad:" + change.source_revision).encode()
                        ).hexdigest()
                        + ":quarantine",
                        metadata=Metadata(
                            source_uri=self.identity.url, source_type="slack-document"
                        ),
                        sections=(Section(text="\0"),),
                    )
                if isinstance(change, Upsert) and any(
                    section.text == "\0" for section in change.sections
                ):
                    bad.add(change.root_id)
                if (
                    document_changes([change]).get_total_buffer_size()
                    > self.context.limits.max_bytes
                ):
                    raise ValueError("slack_batch_limit")
                candidate = {**pending, change.root_id: change}
                targets = (
                    self.migrations.get(change.root_id, set())
                    if isinstance(change, Delete)
                    else set()
                )
                if targets & bad:
                    continue
                # Keep a replacement and its delete together, in that order.
                # If they cannot fit, defer deletion until the replacement's
                # acknowledged envelope is available on the next pass.
                if targets - acknowledged and (
                    not targets.issubset(pending)
                    or any(
                        not isinstance(prepared := pending[key], Upsert)
                        or any(section.text == "\0" for section in prepared.sections)
                        for key in targets
                    )
                    or document_changes(candidate.values()).num_rows > self.context.limits.max_rows
                    or document_changes(candidate.values()).get_total_buffer_size()
                    > self.context.limits.max_bytes
                ):
                    continue
                if pending and (
                    document_changes(candidate.values()).num_rows > self.context.limits.max_rows
                    or document_changes(candidate.values()).get_total_buffer_size()
                    > self.context.limits.max_bytes
                ):
                    yield flush(durable)
                pending[change.root_id] = change
                if isinstance(change, Upsert):
                    self.known[change.root_id] = change.source_revision
                else:
                    self.known.pop(change.root_id, None)
            self.state.channels[identity] = state
        self.state.channels = {
            key: value for key, value in self.state.channels.items() if key in self.channels
        }
        yield flush(self.state.encode())

    def _listing_channel(self, channel, state, latest):
        payloads = history(self.client, channel, 0, latest)
        known_threads = set(self._refs(channel, "thread"))
        messages = self._messages(payloads, known_threads)
        windows = build(
            messages, [WindowAnchor(*a) for a in state.anchors], self.settings.window_gap_seconds
        )
        roots = {
            root(self.identity.workspace, channel.id, "window", item.start) for item in windows
        }
        for stamp in sorted(
            {item.thread for item in messages if item.thread is not None} | known_threads
        ):
            thread = threads.read(
                self.client, channel, stamp, self.scope.bots, self.context.limits.document_max_bytes
            )
            if thread and any(item.eligible for item in thread.messages):
                roots.add(root(self.identity.workspace, channel.id, "thread", stamp))
        # A re-keyed span or a new thread does not justify deleting its old
        # published window until the canonical replacement is acknowledged.
        candidates = {
            root(self.identity.workspace, channel.id, "window", item.start): self._change(
                channel, "window", item.start, item.messages
            )
            for item in windows
        }
        ordered = sorted((item for item in messages if item.eligible), key=lambda item: item.ts)
        stamps = [item.ts for item in ordered]
        owners = {
            item.ts: root(self.identity.workspace, channel.id, "window", window.start)
            for window in windows
            for item in window.messages
        }
        examined = 0
        for key in self.known:
            parts = decode(key, self.identity.workspace)
            if not parts or parts[:2] != (channel.id, "window") or key in roots:
                continue
            match = re.fullmatch(r"[0-9a-f]{64}:([0-9]{1,18})", self.known[key])
            if not match:
                roots.add(key)  # Unknown old span: no destructive inference.
                continue
            end = int(match[1])
            live = ordered[bisect_left(stamps, parts[2]) : bisect_right(stamps, end)]
            examined += len(live)
            if examined > MAX_ENTRIES:
                raise ValueError("slack_replacement_scan_limit")
            replacements = {owners[item.ts] for item in live if item.ts in owners}
            for stamp in {item.thread for item in live if item.thread is not None}:
                thread = threads.read(
                    self.client,
                    channel,
                    stamp,
                    self.scope.bots,
                    self.context.limits.document_max_bytes,
                )
                target = root(self.identity.workspace, channel.id, "thread", stamp)
                if thread:
                    candidates[target] = self._change(channel, "thread", stamp, thread.messages)
                    replacements.add(target)
            if live and (
                not replacements
                or any(
                    target not in candidates
                    or not isinstance(candidates[target], Upsert)
                    or any(section.text == "\0" for section in candidates[target].sections)
                    or self.known.get(target) != candidates[target].source_revision
                    for target in replacements
                )
            ):
                roots.add(key)
        # A pending root born after the listing's as-of fence cannot be deleted.
        for key in self.known:
            parts = decode(key, self.identity.workspace)
            if (
                parts
                and parts[0] == channel.id
                and parts[2] > latest
                and history(self.client, channel, parts[2], parts[2])
            ):
                roots.add(key)
        if Channel.read(self.client, channel.id) != channel:
            raise ValueError("slack_channel_changed_during_listing")
        return roots

    def _listing_ids(self):
        previous = None
        latest = self.client.now
        for _ in range(3):
            roots = set()
            for identity, channel in list(self.channels.items()):
                try:
                    roots.update(
                        self._listing_channel(
                            channel, self.state.channels.get(identity, ChannelCursor()), latest
                        )
                    )
                except ApiError as error:
                    if not error.gone or identity not in self.known_ids:
                        raise
                    self.channels.pop(identity)
                    self.gone.add(identity)
            for identity, channel in list(self.channels.items()):
                try:
                    if Channel.read(self.client, identity) != channel:
                        raise ValueError("slack_channel_changed_during_listing")
                except ApiError as error:
                    if not error.gone or identity not in self.known_ids:
                        raise
                    self.channels.pop(identity)
                    self.gone.add(identity)
            current, gone = catalog(self.client, self.scope, self.known_ids)
            self.gone.update(gone)
            # An authoritative loss during this pass remains out of scope even
            # if conversations.list still returns its stale public metadata.
            current = {key: value for key, value in current.items() if key not in self.gone}
            self.channels = {key: value for key, value in self.channels.items() if key not in gone}
            roots = {key for key in roots if key.split(":")[2] not in self.gone}
            if current != self.channels:
                raise ValueError("slack_catalog_changed_during_listing")
            if roots == previous:
                return roots
            previous = roots
        raise ValueError("slack_listing_did_not_converge")

    def listing(self, cursor):
        # Reconcile has its own cursor. Published root anchors are not a content
        # cursor: reconstruct the bounded seed ledger from live first messages.
        self._open()
        roots = sorted(self._listing_ids())
        listing_id = hashlib.sha256(json.dumps(roots).encode()).hexdigest()
        width = max(1, self.context.limits.max_rows)
        parts = [roots[start : start + width] for start in range(0, len(roots), width)] or [[]]
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

    def _access(self):
        self._open()
        snapshot = AccessSnapshot.read(
            self.client, self.channels, self.scope, self.settings, self.known_ids
        )
        self.gone.update(self.known_ids - self.channels.keys())
        self.labels = {key: value.name for key, value in self.channels.items()}
        return snapshot

    def _check_batches(self, kind, cursor, batches):
        # Preflight every frame before yielding, so no later size error leaves
        # part of the read yielded.
        for batch in batches:
            if header_bytes(self.context.source_id, kind, cursor, batch) > (
                self.context.limits.max_bytes
            ):
                raise ValueError("slack_snapshot_envelope_limit")
            cursor = batch.cursor

    def permissions(self, cursor):
        if self.context.permissions == "fixed":
            return
        access = self._access()
        roots = self._listing_ids() | set(self.known)
        if len(roots) > MAX_ENTRIES:
            raise ValueError("slack_permission_snapshot_limit")
        grants = {
            container(self.identity.workspace, key): values if key in self.channels else ()
            for key, values in access.readers.items()
        }
        documents = []
        for key in sorted(roots):
            pieces = key.split(":")
            binding = (
                container(pieces[1], pieces[2])
                if len(pieces) == 5 and pieces[0] == "slack"
                else None
            )
            if binding:
                grants.setdefault(binding, ())
            documents.append(
                DocumentPermission(
                    root_id=key,
                    permission_revision=hashlib.sha256(
                        ("bind:" + str(binding)).encode()
                    ).hexdigest(),
                    readers=(),
                    containers=(binding,) if binding else (),
                )
            )
        containers = [
            ContainerGrant(
                container_id=key,
                container_kind="slack_channel",
                permission_revision=hashlib.sha256(json.dumps(values).encode()).hexdigest(),
                readers=values,
            )
            for key, values in sorted(grants.items())
        ]
        token = hashlib.sha256(f"{cursor}:{time.time_ns()}".encode()).hexdigest()
        batches = [
            PermissionBatch(documents=documents[start : start + 500], cursor=f"{token}:d:{start}")
            for start in range(0, len(documents), 500)
        ]
        batches += [
            PermissionBatch(
                container_grants=containers[start : start + 64], cursor=f"{token}:g:{start}"
            )
            for start in range(0, len(containers), 64)
        ]
        if not batches:
            batches = [PermissionBatch(cursor=token)]
        self._check_batches(SyncKind.PERMISSIONS, cursor, batches)
        yield from batches

    def members(self, cursor):
        if self.context.permissions == "fixed":
            return
        access = self._access()
        token = hashlib.sha256(f"{cursor}:{time.time_ns()}".encode()).hexdigest()
        pages = [
            access.users[start : start + 500] for start in range(0, len(access.users), 500)
        ] or [()]
        batches = [
            PrincipalBatch(
                snapshot_id=token,
                part=part,
                last=part == len(pages) - 1,
                users=users,
                groups=(access.group,) if part == len(pages) - 1 else (),
                cursor=f"{token}:{part}",
            )
            for part, users in enumerate(pages)
        ]
        self._check_batches(SyncKind.MEMBERS, cursor, batches)
        yield from batches
