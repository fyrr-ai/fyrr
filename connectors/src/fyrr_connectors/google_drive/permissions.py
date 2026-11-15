"""Drive permissions as `readers` tokens, and the Directory as principals.

Pure mapping: what is not an explicit grant to a user, a group or a
discoverable domain gives no token, so a doubtful permission restricts and
never widens. A shared drive's own members never go on a file's rows: the
file carries the drive as a container while it inherits every member, so a
membership change rewrites no rows.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from ..connector import EmailVerification, SourceGroup, SourceUser
from ..validate import MAX_READERS
from .export import clean

PERMISSION_FIELDS = (
    "id,type,emailAddress,domain,allowFileDiscovery,deleted,expirationTime,view,"
    "permissionDetails(inherited,inheritedFrom)"
)
#: The principal ID part of a token stays well inside the contract's limit.
MAX_PRINCIPAL_BYTES = 320


def _principal(value: object) -> str | None:
    text = clean(str(value or "").lower(), MAX_PRINCIPAL_BYTES)
    return text if text and len(text.split()) == 1 else None


def reader(permission: Mapping[str, Any]) -> str | None:
    """The token one Drive permission grants, whatever its role; None for
    link sharing, expiring, deleted and view-limited permissions."""
    if permission.get("deleted") or permission.get("view") or permission.get("expirationTime"):
        return None
    kind = permission.get("type")
    if kind in ("user", "group"):
        address = _principal(permission.get("emailAddress"))
        return address and f"{'usr' if kind == 'user' else 'grp'}:{address}"
    if kind == "domain" and permission.get("allowFileDiscovery") is True:
        domain = _principal(permission.get("domain"))
        return domain and f"grp:domain:{domain}"
    return None


def readers(
    permissions: Sequence[Mapping[str, Any]],
    drive: frozenset[str] | None = None,
    drive_id: str | None = None,
) -> tuple[list[str], bool]:
    """A file's row readers and whether its permission list states that it
    inherits the whole shared drive.

    `drive` holds the permission IDs of the shared drive `drive_id` itself. A
    permission is the drive's membership only where the API says so: a detail
    inherited from the drive. The same ID granted on the file directly or
    through a folder is a reader of the file. The list states the inheritance
    when every member is on the file as membership; that an ID is present is
    no evidence. Without it (a limited-access folder) the memberships the
    file still shows are dropped rather than frozen onto the rows, and so is
    a member's permission whose origin the API does not give.
    """

    def details(permission: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
        return permission.get("permissionDetails") or ()

    def from_drive(detail: Mapping[str, Any]) -> bool:
        return detail.get("inherited") is True and detail.get("inheritedFrom") == drive_id

    def granted(detail: Mapping[str, Any]) -> bool:
        """On the file itself, or inherited from a named folder."""
        if detail.get("inherited") is False:
            return True
        return detail.get("inherited") is True and detail.get("inheritedFrom") not in (
            None,
            drive_id,
        )

    own = [
        permission
        for permission in permissions
        if drive is None
        or permission.get("id") not in drive
        or any(map(granted, details(permission)))
    ]
    members = {
        permission.get("id")
        for permission in permissions
        if any(map(from_drive, details(permission)))
    }
    inherits = drive is not None and drive_id is not None and drive <= members
    return sorted({token for token in map(reader, own) if token}), inherits


def revision(tokens: Iterable[str], containers: Iterable[str] = ()) -> str:
    """Drive has no permission version: the mapped state identifies itself."""
    state = json.dumps([sorted(tokens), sorted(containers)]).encode()
    return hashlib.sha256(state).hexdigest()[:32]


def principals(
    users: Iterable[Mapping[str, Any]], groups: Mapping[str, Iterable[Mapping[str, Any]]]
) -> tuple[list[SourceUser], list[SourceGroup]]:
    """Directory users and groups (direct members; fyrr expands nesting), plus
    one `domain:<domain>` group per domain holding its active users."""
    people: list[SourceUser] = []
    domains: dict[str, list[str]] = {}
    for user in users:
        address = _principal(user.get("primaryEmail"))
        if address is None:
            continue
        active = not (user.get("suspended") or user.get("archived"))
        people.append(
            SourceUser(
                id=address,
                active=active,
                email=address,
                email_verification=EmailVerification.VERIFIED_CORPORATE,
            )
        )
        if active and "@" in address:
            domains.setdefault(address.rsplit("@", 1)[1], []).append(f"usr:{address}")
    everyone = [f"grp:domain:{domain}" for domain in sorted(domains)]
    found: list[SourceGroup] = []
    for group, members in groups.items():
        address = _principal(group)
        if address is None:
            continue
        refs: set[str] = set()
        for member in members:
            kind, member_address = member.get("type"), _principal(member.get("email"))
            if kind == "CUSTOMER":
                refs.update(everyone)
            elif kind in ("USER", "GROUP") and member_address:
                refs.add(f"{'usr' if kind == 'USER' else 'grp'}:{member_address}")
        found.append(SourceGroup(id=address, members=sorted(refs)))
    found += [
        SourceGroup(id=f"domain:{domain}", members=sorted(domains[domain]))
        for domain in sorted(domains)
    ]
    if any(len(group.members) > MAX_READERS for group in found):
        raise ValueError("google_drive_group_too_large")
    return people, found
