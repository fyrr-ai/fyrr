"""Pinned scoped-token gateway and explicit Cloud issue selectors."""

import re
from urllib.parse import urlsplit
from uuid import UUID

from ..config import ConfigError, ConfigModel, Secret, secret, setting


class Settings(ConfigModel):
    site_url: str = setting(description="Canonical HTTPS Jira Cloud site")
    cloud_id: str = setting(description="Expected Cloud UUID")
    api_token: Secret = secret(description="Scoped service-account API token environment reference")
    api_endpoint: str = setting(
        description="Trusted proxy or synthetic loopback origin", default=""
    )
    corporate_domains: list[str] = setting(
        description="Approved automatic account-link domains", default_factory=list, max_items=64
    )

    def __post_init__(self):
        try:
            UUID(self.cloud_id)
        except (ValueError, TypeError):
            raise ConfigError("settings.cloud_id", "must be a Cloud UUID") from None
        for key, value in (("site_url", self.site_url), ("api_endpoint", self.api_endpoint)):
            if not value and key == "api_endpoint":
                continue
            parsed = urlsplit(value)
            if (
                not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or parsed.path not in ("", "/")
                or (
                    parsed.scheme != "https"
                    and not (
                        parsed.scheme == "http"
                        and parsed.hostname in ("localhost", "127.0.0.1", "::1")
                    )
                )
            ):
                raise ConfigError(
                    "settings." + key, "must be a bounded HTTPS origin or loopback fake"
                )
        if any(
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", d)
            or "." not in d
            or ".." in d
            for d in self.corporate_domains
        ):
            raise ConfigError("settings.corporate_domains", "must be lowercase domain names")


class Scope(ConfigModel):
    projects: list[int] = setting(
        description="Stable project IDs, mutually exclusive with jql",
        default_factory=list,
        max_items=256,
    )
    jql: str = setting(description="Bounded static JQL expression without ORDER BY", default="")
    issue_types: list[int] = setting(
        description="Issue type IDs, empty includes all", default_factory=list, max_items=256
    )
    labels: list[str] = setting(
        description="Any-of exact labels", default_factory=list, max_items=64
    )
    skip_comment_authors: list[str] = setting(
        description="Opaque account IDs to omit", default_factory=list, max_items=256
    )
    fields: list[str] = setting(
        description="Custom field IDs to index as text, in this order",
        default_factory=list,
        max_items=16,
    )

    def __post_init__(self):
        if bool(self.projects) == bool(self.jql):
            raise ConfigError("scope", "exactly one of projects or jql is required")
        if any(
            type(v) is not int or not 0 < v < 10**18 for v in (*self.projects, *self.issue_types)
        ) or len(set(self.projects)) != len(self.projects):
            raise ConfigError("scope.projects", "IDs must be positive and unique")
        if (
            len(self.jql.encode()) > 8192
            or re.search(r"\b(order\s+by|filter|now|currentUser|membersOf)\b", self.jql, re.I)
            or any(v.lower() != "in" for v in re.findall(r"\b([a-zA-Z_]\w*)\s*\(", self.jql))
            or "\0" in self.jql
        ):
            raise ConfigError(
                "scope.jql", "must be a bounded static expression without saved filters or ordering"
            )
        if any(
            not v or len(v.encode()) > 256 or any(ord(c) < 32 for c in v)
            for v in (*self.labels, *self.skip_comment_authors)
        ):
            raise ConfigError("scope", "selectors must be bounded nonempty strings")
        if any(not re.fullmatch(r"customfield_[0-9]{1,18}", v) for v in self.fields) or len(
            set(self.fields)
        ) != len(self.fields):
            raise ConfigError("scope.fields", "must be unique custom field IDs")

    def query(self):
        return self.jql or "project in (" + ",".join(map(str, self.projects)) + ")"

    def matches(self, issue):
        fields = issue["fields"]
        return (
            (not self.projects or int(fields["project"]["id"]) in self.projects)
            and (not self.issue_types or int(fields["issuetype"]["id"]) in self.issue_types)
            and (not self.labels or bool(set(fields["labels"]) & set(self.labels)))
        )
