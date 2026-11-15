"""Complete workspace full-member group and channel-specific human grants."""

import re
from dataclasses import dataclass

from ..connector import EmailVerification, SourceGroup, SourceUser
from ..validate import MAX_READERS
from .client import MAX_ENTRIES, ApiError
from .feeds import Channel


def user_id(workspace, identity):
    return f"slack.com:{workspace}:{identity}"


def workspace_group(workspace):
    return f"slack.com:{workspace}:workspace"


def container(workspace, identity):
    return f"slack:{workspace}:channel:{identity}"


def account(body, settings):
    identity = body.get("id")
    if not isinstance(identity, str) or not re.fullmatch(r"[UW][A-Z0-9]{2,31}", identity):
        raise ValueError("slack_member_identity_invalid")
    flags = [body.get(key) for key in ("deleted", "is_restricted", "is_ultra_restricted", "is_bot")]
    if body.get("team_id") != settings.workspace_id or any(
        not isinstance(value, bool) for value in flags
    ):
        raise ValueError("slack_member_workspace_or_flags_invalid")
    profile = body.get("profile", {})
    if not isinstance(profile, dict):
        raise ValueError("slack_member_profile_invalid")
    raw = profile.get("email")
    email = None
    if raw is not None:
        if not isinstance(raw, str):
            raise ValueError("slack_member_email_invalid")
        try:
            valid = len(raw.encode()) <= 320
        except UnicodeError:
            valid = False
        if (
            valid
            and re.fullmatch(r"[^@\s\x00-\x1f]+@[^@\s\x00-\x1f]+", raw)
            and raw.rsplit("@", 1)[1].lower() in settings.corporate_domains
            and not flags[3]
        ):
            email = raw
    return SourceUser(
        id=user_id(settings.workspace_id, identity),
        active=not flags[0] and not flags[3],
        email=email,
        email_verification=EmailVerification.VERIFIED_CORPORATE
        if email
        else EmailVerification.UNVERIFIED,
    ), (flags[1] or flags[2])


@dataclass(frozen=True)
class AccessSnapshot:
    readers: dict[str, tuple[str, ...]]
    users: tuple[SourceUser, ...]
    group: SourceGroup

    @classmethod
    def read(cls, client, channels, scope, settings, known_ids):
        users, guests = {}, {}
        count = 0
        for entries in client.pages("users.list", "members"):
            for body in entries:
                count += 1
                if count > MAX_ENTRIES:
                    raise ValueError("slack_member_snapshot_limit")
                # Foreign Slack Connect identities do not form this workspace's floor.
                if body.get("team_id") != settings.workspace_id and body.get("is_stranger") is True:
                    continue
                parsed, guest = account(body, settings)
                identity = body["id"]
                users[identity], guests[identity] = parsed, guest
        listed_users = set(users)
        grants = {}
        for identity in list(channels):
            members = set()
            try:
                for entries in client.pages("conversations.members", "members", channel=identity):
                    for member in entries:
                        count += 1
                        if count > MAX_ENTRIES:
                            raise ValueError("slack_acl_snapshot_limit")
                        if not isinstance(member, str) or not re.fullmatch(
                            r"[UW][A-Z0-9]{2,31}", member
                        ):
                            raise ValueError("slack_member_identity_invalid")
                        members.add(member)
                # One membership snapshot; the final flags decide the grant,
                # including public/private conversion during those pages.
                after = Channel.read(client, identity)
            except ApiError as error:
                if not error.gone or identity not in known_ids:
                    raise
                channels.pop(identity)
                grants[identity] = ()
                continue
            if not scope.matches(after):
                channels.pop(identity)
                grants[identity] = ()
                continue
            channels[identity] = after
            for member in sorted(members - users.keys()):
                body = client.get("users.info", user=member).get("user")
                if not isinstance(body, dict) or body.get("id") != member:
                    raise ValueError("slack_member_identity_invalid")
                parsed, guest = account(body, settings)
                users[member], guests[member] = parsed, guest
            readers = [
                "usr:" + users[member].id
                for member in sorted(members)
                if users[member].active and (after.private or guests[member])
            ]
            if not after.private:
                readers.append("grp:" + workspace_group(settings.workspace_id))
            if len(readers) > MAX_READERS:
                raise ValueError("slack_channel_readers_limit")
            grants[identity] = tuple(sorted(readers))
        full = sorted(
            "usr:" + value.id
            for key, value in users.items()
            if key in listed_users and value.active and not guests[key]
        )
        if len(full) > MAX_READERS:
            raise ValueError("slack_workspace_group_limit")
        return cls(
            grants,
            tuple(users[key] for key in sorted(users)),
            SourceGroup(id=workspace_group(settings.workspace_id), members=tuple(full)),
        )
