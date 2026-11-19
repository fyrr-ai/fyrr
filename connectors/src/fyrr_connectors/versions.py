"""Versions of the SDK and of the contracts it speaks.

The SDK version is semantic: a provider declares the oldest SDK it works with
(`min_sdk_version`), and fyrr refuses a provider whose minimum is in another
compatibility series (`sdk_series`) than the one it loads. The contract
versions are `major.minor` and must equal the Rust contracts (`schemas/*.json`,
`x-fyrr-schema-version`); the tests check it.
"""

#: The SDK version; equals the `fyrr-connectors` distribution version.
SDK_VERSION = "0.1.1"
#: The connector contract (batches, cursors, Arrow document changes).
CONNECTOR_CONTRACT_VERSION = "1.3"
#: The provider metadata contract (`schemas/connector-provider.schema.json`).
PROVIDER_CONTRACT_VERSION = "2.0"
#: The entry point group providers register under.
ENTRY_POINT_GROUP = "fyrr.connectors"


def sdk_series(version: str) -> str:
    """Return the compatibility series of a `major.minor.patch` SDK version.

    Semantic Versioning decides compatibility by the leading non-zero
    component: `0.y.z` is series `0.y`, and `x.y.z` with `x > 0` is series `x`.
    """
    major, minor = (int(part) for part in version.split(".")[:2])
    return f"0.{minor}" if major == 0 else str(major)


#: The SDK series, the compatibility promise to providers.
SDK_SERIES = sdk_series(SDK_VERSION)


def contract_major(version: str = CONNECTOR_CONTRACT_VERSION) -> int:
    """Return the major number of a `major.minor` contract version."""
    major, minor = version.split(".")
    if not (major.isdigit() and minor.isdigit()):
        raise ValueError("contract versions are <major>.<minor>")
    return int(major)


def accepts(version: str) -> bool:
    """Whether this SDK reads a document of `version`: same major, any minor."""
    return contract_major(version) == contract_major()
