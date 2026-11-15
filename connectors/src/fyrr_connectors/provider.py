"""Provider metadata, as Airflow's `get_provider_info`.

A provider distribution registers a callable under the `fyrr.connectors`
entry point group, named after the provider (the value of a source's
`type`):

    [project.entry-points."fyrr.connectors"]
    markdown-dir = "my_package:provider_info"

The callable returns `provider_info(...)`. fyrr validates the result against
the published provider schema and takes the provider's version from the
distribution, never from the metadata.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .connector import Connector
from .versions import PROVIDER_CONTRACT_VERSION


def provider_info(
    *,
    name: str,
    connector: type[Connector],
    min_sdk_version: str,
    connection_types: Sequence[str] = ("sync", "fixed"),
    description: str | None = None,
) -> dict[str, Any]:
    """Metadata for one provider.

    `connection_types` are the permissions modes the connector supports:
    `sync` (it maps the source's permissions) and/or `fixed` (the source's
    configured readers apply). The sync kinds and the config schema come from
    the connector class (`Settings`, `Scope`, overridden methods).
    """
    if not (isinstance(connector, type) and issubclass(connector, Connector)):
        raise TypeError("connector must be a Connector subclass")
    info: dict[str, Any] = {
        "schema_version": PROVIDER_CONTRACT_VERSION,
        "name": name,
    }
    if description is not None:
        info["description"] = description
    info.update(
        min_sdk_version=min_sdk_version,
        connection_types=list(connection_types),
        sync_kinds=[str(kind) for kind in connector.sync_kinds()],
        connector=f"{connector.__module__}:{connector.__qualname__}",
        config_schema={
            "settings": connector.Settings.json_schema(),
            "scope": connector.Scope.json_schema(),
        },
    )
    return info
