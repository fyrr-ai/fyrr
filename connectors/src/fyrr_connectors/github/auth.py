"""App identity proof and renewable read-only installation credentials."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .client import Client, instant, number
from .config import Settings


@dataclass(frozen=True)
class Installation:
    id: int
    account_id: int


class AppAuth:
    def __init__(self, client: Client, settings: Settings):
        self.client, self.settings = client, settings
        self._token = ""
        self._expires = datetime.min.replace(tzinfo=UTC)
        body, _ = client.request(
            "GET",
            f"/app/installations/{settings.installation_id}",
            token=self.jwt(),
        )
        if (
            not isinstance(body, dict)
            or number(body.get("id")) != settings.installation_id
            or number(body.get("app_id")) != settings.app_id
            or not isinstance(body.get("account"), dict)
            or number(body["account"].get("id")) != settings.account_id
            or body.get("suspended_at")
        ):
            raise ValueError("github_installation_mismatch")
        self.installation = Installation(settings.installation_id, settings.account_id)
        client.token, client.refresh = self.token, self.refresh

    def jwt(self) -> str:
        import jwt

        now = datetime.now(UTC)
        try:
            return jwt.encode(
                {
                    "iat": now - timedelta(seconds=60),
                    "exp": now + timedelta(minutes=9),
                    "iss": str(self.settings.app_id),
                },
                self.settings.private_key.reveal(),
                algorithm="RS256",
            )
        except (jwt.PyJWTError, ValueError, TypeError):
            raise ValueError("github_app_key_invalid") from None

    def refresh(self) -> None:
        body, _ = self.client.request(
            "POST",
            f"/app/installations/{self.settings.installation_id}/access_tokens",
            token=self.jwt(),
            body={
                "permissions": {
                    "contents": "read",
                    "issues": "read",
                    "pull_requests": "read",
                    "metadata": "read",
                }
            },
        )
        if (
            not isinstance(body, dict)
            or not isinstance(body.get("token"), str)
            or not body["token"]
        ):
            raise ValueError("github_token_invalid")
        expires = instant(body.get("expires_at"))
        if expires is None or expires <= datetime.now(UTC) + timedelta(seconds=60):
            raise ValueError("github_token_invalid")
        permissions = body.get("permissions")
        if not isinstance(permissions, dict) or any(
            value != "read" for value in permissions.values()
        ):
            raise ValueError("github_token_permissions_invalid")
        self._token, self._expires = body["token"], expires

    def token(self) -> str:
        if datetime.now(UTC) + timedelta(seconds=60) >= self._expires:
            self.refresh()
        return self._token
