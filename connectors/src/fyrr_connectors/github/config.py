"""GitHub App settings and explicit repository scope."""

from ..config import ConfigError, ConfigModel, Secret, secret, setting


class Settings(ConfigModel):
    app_id: int = setting(description="Expected GitHub App ID", minimum=1)
    installation_id: int = setting(description="Expected installation ID", minimum=1)
    account_id: int = setting(description="Expected installation owner ID", minimum=1)
    private_key: Secret = secret(description="RS256 App private key PEM environment reference")
    api_endpoint: str = setting(
        description="REST origin; change only for a synthetic test server or trusted proxy",
        default="https://api.github.com",
        pattern="^https?://[^/?#]+$",
    )


class RepositoryScope(ConfigModel):
    id: int = setting(description="Stable numeric repository ID", minimum=1)
    branch: str = setting(description="Explicit branch; no default-branch drift")
    issues: bool = setting(description="Include issues and comments", default=True)
    pull_requests: bool = setting(
        description="Include PRs, submitted reviews and comments", default=True
    )
    prose: bool = setting(
        description="Include Markdown, reStructuredText and prose text", default=True
    )
    code: bool = setting(
        description="Opt in to source code text for this repository", default=False
    )

    def __post_init__(self):
        if not self.branch or len(self.branch.encode()) > 1024 or "\0" in self.branch:
            raise ConfigError("scope.repositories.branch", "must be a bounded nonempty branch")
        if not any((self.issues, self.pull_requests, self.prose, self.code)):
            raise ConfigError("scope.repositories", "must enable at least one content kind")


class Scope(ConfigModel):
    repositories: list[RepositoryScope] = setting(
        description="Repositories, branches and per-repository content toggles",
        min_items=1,
        max_items=64,
    )

    def __post_init__(self):
        if len({repo.id for repo in self.repositories}) != len(self.repositories):
            raise ConfigError("scope.repositories", "repository IDs must be unique")
