"""Located field/comment units, source metadata and whole-document quarantine."""

import hashlib
import json
from datetime import datetime

from ..arrow import Location, Metadata, Section
from ..validate import MAX_LABELS
from .adf import ADFError, text
from .comments import visibility_signature
from .feeds import IssueBinding

FORMAT = "jira-1"


def date(value):
    if value is None:
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError
        return result
    except (ValueError, TypeError, AttributeError):
        raise ADFError() from None


def document(identity, project, issue, comments, context, targets=(), dynamic=False, groups=()):
    fields = issue["fields"]
    uri = identity.site + "/browse/" + issue["key"]
    sections = []
    # The key opens the summary so excerpts and key queries carry it.
    summary = f"{issue['key']} {text(fields.get('summary'))}"
    field_values = {"summary": summary, "description": fields.get("description")}
    for key in ("status", "issuetype", "priority", "reporter", "assignee"):
        value = fields.get(key)
        if isinstance(value, dict):
            label = (
                value.get("displayName") if key in ("reporter", "assignee") else value.get("name")
            )
            if label:
                field_values[key] = label
    if fields.get("labels"):
        field_values["labels"] = ", ".join(fields["labels"])
    # Only under fixed readers: Jira hides a link from a reader who may not see
    # the linked issue, and a synced audience covers this issue alone.
    if context.permissions == "fixed":
        lines = [f"parent: {fields['parent']['key']}"] if fields.get("parent") else []
        lines += sorted(
            f"{entry['type'][side]}: {entry[side + 'Issue']['key']}"
            for entry in fields.get("issuelinks") or ()
            for side in ("inward", "outward")
            if entry.get(side + "Issue")
        )
        if lines:
            field_values["links"] = "\n".join(lines)
    for key, value in field_values.items():
        body = text(value)
        if len(body.encode()) > context.limits.document_max_bytes:
            raise ADFError()
        for partno, unit in enumerate(units(body)):
            sections.append(
                Section(text=unit, location=Location(kind="field", label=key, index=partno))
            )
    for comment in comments:
        if comment.restricted and context.permissions == "fixed":
            continue
        if project.kind == "service_desk" and context.permissions == "fixed":
            continue
        body = text(comment.body.get("body"))
        if not body:
            continue
        readers: tuple[str, ...] | None = None
        if comment.restricted:
            target = next((item for item in targets if item[0] == comment.id), None)
            if target is None:
                readers = ()
            else:
                from .permissions import policy_token

                readers = (
                    policy_token(identity.cloud, project.id, number_level(fields), target[1:]),
                )
        if len(body.encode()) > context.limits.document_max_bytes:
            raise ADFError()
        for partno, unit in enumerate(units(body)):
            sections.append(
                Section(
                    text=unit,
                    readers=readers,
                    location=Location(
                        kind="comment", label=f"{uri}?focusedCommentId={comment.id}", index=partno
                    ),
                )
            )
    if sum(len(section.text.encode()) for section in sections) > context.limits.document_max_bytes:
        raise ADFError()
    author = (
        fields.get("reporter", {}).get("displayName")
        if isinstance(fields.get("reporter"), dict)
        else None
    )
    meta = Metadata(
        source_uri=uri,
        source_type="jira-issue",
        title=text(fields.get("summary"))[:256],
        author=text(author) or None,
        project=project.name,
        status=fields["status"].get("name") if isinstance(fields.get("status"), dict) else None,
        labels=sorted(set(fields.get("labels", [])))[:MAX_LABELS],
        created_at=date(fields.get("created")),
        updated_at=date(fields.get("updated")),
    )
    binding = IssueBinding(
        project.id,
        number_level(fields),
        dynamic,
        tuple(sorted({tuple(item[1:]) for item in targets})),
        groups,
        visibility_signature(comments) if context.permissions == "sync" else None,
    )
    canonical = [
        FORMAT,
        issue["key"],
        project.__dict__,
        meta.__dict__,
        [(s.text, s.location.__dict__ if s.location else None, s.readers) for s in sections],
        context.readers if context.permissions == "fixed" else (),
    ]
    digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, default=str).encode()).hexdigest()
    return binding.revision(digest), meta, sections


def number_level(fields):
    from .client import number

    value = fields.get("security")
    return number(value["id"]) if value else None


def units(body):
    part: list[str] = []
    used = 0
    for character in body:
        count = len(character.encode())
        if used + count > 16384:
            yield "".join(part)
            part = []
            used = 0
        part.append(character)
        used += count
    if part:
        yield "".join(part)
