"""Slack connection and explicitly configured channel scope."""

import re
from fnmatch import fnmatchcase

from ..config import ConfigError, ConfigModel, Secret, secret, setting


class Settings(ConfigModel):
    token: Secret = secret(description="Workspace user OAuth token environment reference")
    workspace_id: str = setting(
        description="Expected Slack workspace ID", pattern="^T[A-Z0-9]{2,31}$"
    )
    window_gap_seconds: int = setting(
        description="Conversation gap in seconds", default=300, minimum=1, maximum=3600
    )
    corporate_domains: list[str] = setting(
        description="Operator-verified corporate account domains; empty requires explicit links",
        default_factory=list,
        max_items=64,
    )
    api_endpoint: str = setting(
        description="Slack REST origin; override only for a synthetic server or trusted proxy",
        default="https://slack.com",
        pattern="^https?://[^/?#]+$",
    )

    def __post_init__(self):
        if any(
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\.[a-z]{2,63}", value)
            or ".." in value
            for value in self.corporate_domains
        ):
            raise ConfigError(
                "settings.corporate_domains", "must contain bounded lowercase domains"
            )


class Scope(ConfigModel):
    include: list[str] = setting(
        description="Explicit channel IDs or name globs", min_items=1, max_items=64
    )
    exclude: list[str] = setting(
        description="Excluded IDs or name globs; exclude wins", default_factory=list, max_items=64
    )
    channel_types: list[str] = setting(
        description="public_channel and/or private_channel; no DMs",
        default_factory=lambda: ["public_channel", "private_channel"],
        min_items=1,
        max_items=2,
    )
    bots: bool = setting(description="Include bot text, including within threads", default=False)
    include_shared: bool = setting(
        description="Opt into shared channels under fixed readers only", default=False
    )

    def __post_init__(self):
        if set(self.channel_types) - {"public_channel", "private_channel"}:
            raise ConfigError("scope.channel_types", "DMs and group DMs are unsupported")
        if any(
            not value or len(value.encode()) > 128 or any(c in value for c in "\x00/\\[]")
            for value in (*self.include, *self.exclude)
        ):
            raise ConfigError("scope.include", "must contain bounded IDs or simple name globs")

    def matches(self, channel) -> bool:
        kind = "private_channel" if channel.private else "public_channel"

        def match(pattern):
            return pattern == channel.id or fnmatchcase(channel.name, pattern)

        return (
            kind in self.channel_types
            and any(map(match, self.include))
            and not any(map(match, self.exclude))
            and (not channel.shared or self.include_shared)
        )
