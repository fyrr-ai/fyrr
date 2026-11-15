"""One verified workspace user-token identity, without directory reads."""

import re
from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Identity:
    workspace: str
    url: str

    @classmethod
    def read(cls, client, settings):
        if not client.token.startswith("xoxp-"):
            raise ValueError("slack_user_token_required")
        body = client.get("auth.test")
        url = body.get("url")
        if (
            body.get("team_id") != settings.workspace_id
            or body.get("bot_id")
            or not re.fullmatch(r"[UW][A-Z0-9]{2,31}", str(body.get("user_id", "")))
        ):
            raise ValueError("slack_workspace_identity_invalid")
        parsed = urlsplit(url) if isinstance(url, str) else None
        if (
            parsed is None
            or parsed.scheme != "https"
            or not parsed.hostname
            or not parsed.hostname.endswith(".slack.com")
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("slack_workspace_url_invalid")
        return cls(settings.workspace_id, url.rstrip("/"))
