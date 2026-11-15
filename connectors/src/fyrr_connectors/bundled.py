"""The bundled providers: the v1 connectors register under the same
entry point group and SDK as any external provider.

Local files acquire UTF-8 documents; Google Drive exports Docs, Sheets and
Slides; Notion reads pages and database rows; GitHub reads conversations and
branch files; Slack reads channel windows and threads; Jira reads issues with
separate comment audiences.
"""

from __future__ import annotations

from typing import Any

from .local_files import LocalFiles
from .provider import provider_info


def local_files() -> dict[str, Any]:
    return provider_info(
        name="local-files",
        connector=LocalFiles,
        min_sdk_version="0.1.0",
        connection_types=("fixed",),
        description="Local UTF-8 text files",
    )


def _cloud_info(name, class_name, description, *, fixed=False, dependencies=("requests",)):
    # Configuration modules depend only on the SDK and standard library. Package
    # exports resolve the real connector only when a worker opens the source.
    from importlib import import_module
    from importlib.util import find_spec

    from .versions import PROVIDER_CONTRACT_VERSION

    module = name.replace("-", "_")
    try:
        if any(find_spec(dependency) is None for dependency in dependencies):
            raise ModuleNotFoundError
        import_module("." + module, __package__)
        config = import_module("." + module + ".config", __package__)
    except ModuleNotFoundError as error:
        raise ImportError(f"the {name} provider needs the fyrr-connectors[{name}] extra") from error
    return {
        "schema_version": PROVIDER_CONTRACT_VERSION,
        "name": name,
        "description": description,
        "min_sdk_version": "0.1.0",
        "connection_types": ["fixed"] if fixed else ["sync", "fixed"],
        "sync_kinds": ["content", "reconcile"]
        if fixed
        else ["content", "permissions", "members", "reconcile"],
        "connector": f"fyrr_connectors.{module}.connector:{class_name}",
        "config_schema": {
            "settings": config.Settings.json_schema(),
            "scope": config.Scope.json_schema(),
        },
    }


def jira() -> dict[str, Any]:
    return _cloud_info(
        "jira", "Jira", "Jira Cloud issues and comments with project and security policy groups"
    )


def slack() -> dict[str, Any]:
    return _cloud_info(
        "slack", "Slack", "Slack channel windows and threads with workspace and channel permissions"
    )


def google_drive() -> dict[str, Any]:
    return _cloud_info(
        "google-drive",
        "GoogleDrive",
        "Google Docs, Sheets and Slides exported to text",
        dependencies=("requests", "google.auth", "openpyxl"),
    )


def github() -> dict[str, Any]:
    return _cloud_info(
        "github",
        "GitHub",
        "GitHub issues, PRs and branch prose; code opt-in, private synced permissions",
        dependencies=("requests", "jwt"),
    )


def notion() -> dict[str, Any]:
    return _cloud_info(
        "notion",
        "Notion",
        "Notion pages and database rows under the configured root pages",
        fixed=True,
    )
