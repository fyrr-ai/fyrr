"""Example fyrr connector provider: Markdown files in a local directory."""

from typing import Any

from fyrr_connectors import provider_info as _provider_info

from .connector import MarkdownConnector

__all__ = ["MarkdownConnector", "provider_info"]


def provider_info() -> dict[str, Any]:
    """The `fyrr.connectors` entry point."""
    return _provider_info(
        name="markdown-dir",
        connector=MarkdownConnector,
        min_sdk_version="0.1.0",
        connection_types=("fixed",),
        description="Markdown files in a local directory (example provider)",
    )
