"""The `settings` and `scope` tables of a Notion source."""

from ..config import ConfigModel, Secret, secret, setting


class Settings(ConfigModel):
    token: Secret = secret(
        description="Secret of the internal Notion integration the root pages are shared with"
    )
    api_endpoint: str = setting(
        description="Notion API base URL; change it only for a test fake or a proxy",
        default="https://api.notion.com",
        pattern="^https?://[^/?#]+$",
    )


class Scope(ConfigModel):
    root_pages: list[str] = setting(
        description="IDs of the pages whose subtrees are read, with or without dashes",
        min_items=1,
    )
