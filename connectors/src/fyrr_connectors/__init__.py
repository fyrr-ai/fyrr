"""fyrr connector SDK.

A connector provider is a Python distribution that registers a callable under
the `fyrr.connectors` entry point group; the callable returns
`provider_info(...)` for a `Connector` subclass. fyrr's full build discovers
installed providers, validates their metadata against the published provider
schema, refuses another SDK series, and runs one sync kind of one source per
worker process, in the Python provider host (`fyrr_connectors.host`) that the
worker starts, receiving content as Arrow IPC streams.

The SDK is semantically versioned (`SDK_VERSION`): its series (`SDK_SERIES`,
`0.y` for `0.y.z`, `x` for `x.y.z`) is the compatibility promise.
`fyrr_connectors.testkit` checks a provider against the published contract in
the provider's own tests.
"""

from .arrow import (
    DOCUMENT_CHANGES_SCHEMA,
    Delete,
    Location,
    Metadata,
    Section,
    Upsert,
    check_document_changes,
    document_changes,
)
from .config import ConfigError, ConfigModel, NoConfig, Secret, secret, setting
from .connector import (
    Connector,
    ContainerGrant,
    ContentBatch,
    DocumentPermission,
    EmailVerification,
    Limits,
    ListingBatch,
    PermissionBatch,
    PrincipalBatch,
    SourceContext,
    SourceGroup,
    SourceUser,
    SyncKind,
)
from .provider import provider_info
from .validate import ContractViolation
from .versions import (
    CONNECTOR_CONTRACT_VERSION,
    ENTRY_POINT_GROUP,
    PROVIDER_CONTRACT_VERSION,
    SDK_SERIES,
    SDK_VERSION,
    accepts,
    contract_major,
    sdk_series,
)

__all__ = [
    "CONNECTOR_CONTRACT_VERSION",
    "DOCUMENT_CHANGES_SCHEMA",
    "ENTRY_POINT_GROUP",
    "PROVIDER_CONTRACT_VERSION",
    "SDK_SERIES",
    "SDK_VERSION",
    "ConfigError",
    "ConfigModel",
    "Connector",
    "ContainerGrant",
    "ContentBatch",
    "ContractViolation",
    "Delete",
    "DocumentPermission",
    "EmailVerification",
    "Limits",
    "ListingBatch",
    "Location",
    "Metadata",
    "NoConfig",
    "PermissionBatch",
    "PrincipalBatch",
    "Secret",
    "Section",
    "SourceContext",
    "SourceGroup",
    "SourceUser",
    "SyncKind",
    "Upsert",
    "accepts",
    "check_document_changes",
    "contract_major",
    "document_changes",
    "provider_info",
    "sdk_series",
    "secret",
    "setting",
]
