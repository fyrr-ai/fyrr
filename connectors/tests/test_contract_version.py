"""The Python SDK and the Rust contracts agree on versions and schemas."""

import json
from importlib import metadata
from pathlib import Path

import pytest

from fyrr_connectors import (
    CONNECTOR_CONTRACT_VERSION,
    PROVIDER_CONTRACT_VERSION,
    SDK_SERIES,
    SDK_VERSION,
    accepts,
    contract_major,
    sdk_series,
)

REPO = Path(__file__).resolve().parents[2]
PACKAGED = REPO / "connectors/src/fyrr_connectors/schemas"


def test_sdk_version_matches_the_rust_contract() -> None:
    schema = json.loads((REPO / "schemas/connector-batch.schema.json").read_text())
    assert schema["x-fyrr-contract"] == "connector-batch"
    assert schema["x-fyrr-schema-version"] == CONNECTOR_CONTRACT_VERSION


def test_provider_contract_and_sdk_series_match_the_rust_build() -> None:
    schema = json.loads((REPO / "schemas/connector-provider.schema.json").read_text())
    assert schema["x-fyrr-contract"] == "connector-provider"
    assert schema["x-fyrr-schema-version"] == PROVIDER_CONTRACT_VERSION
    assert schema["x-fyrr-sdk-series"] == SDK_SERIES


def test_sdk_version_is_the_distribution_version() -> None:
    assert metadata.version("fyrr-connectors") == SDK_VERSION
    assert sdk_series(SDK_VERSION) == SDK_SERIES


@pytest.mark.parametrize(
    ("version", "series"),
    [
        ("0.1.0", "0.1"),
        ("0.1.7", "0.1"),
        ("0.2.0", "0.2"),
        ("0.0.3", "0.0"),
        ("1.0.0", "1"),
        ("1.4.2", "1"),
        ("2.0.0", "2"),
    ],
)
def test_sdk_series_is_the_leading_non_zero_component(version: str, series: str) -> None:
    assert sdk_series(version) == series


def test_packaged_schemas_equal_the_published_ones() -> None:
    packaged = sorted(path.name for path in PACKAGED.glob("*.json"))
    assert packaged == [
        "connector-batch.schema.json",
        "connector-document-changes.arrow.json",
        "connector-provider.schema.json",
    ]
    for name in packaged:
        assert (PACKAGED / name).read_text() == (REPO / "schemas" / name).read_text(), name


@pytest.mark.parametrize(
    ("fixture", "accepted"),
    [
        ("connector-batch/document-changes.json", True),
        ("compat/connector-batch-minor-addition.json", True),
        ("compat/connector-batch-unknown-major.json", False),
    ],
)
def test_golden_fixtures_follow_the_major_rule(fixture: str, accepted: bool) -> None:
    document = json.loads((REPO / "fixtures/contracts" / fixture).read_text())
    assert accepts(document["schema_version"]) is accepted


def test_malformed_versions_are_refused() -> None:
    assert contract_major("1.0") == 1
    for bad in ["1", "a.b", "1.0.0"]:
        with pytest.raises(ValueError):
            contract_major(bad)


def test_source_email_evidence_defaults_closed_and_validates() -> None:
    from fyrr_connectors import EmailVerification, SourceUser
    from fyrr_connectors.runtime import _plain
    from fyrr_connectors.validate import ContractViolation, _check_user

    user = SourceUser(id="source-user", email="user@example.test", active=True)
    assert user.email_verification == EmailVerification.UNVERIFIED
    assert "email_verification" not in _plain(user)
    for evidence in EmailVerification:
        value = {"id": "source-user", "email": "user@example.test", "email_verification": evidence}
        _check_user(value)
    with pytest.raises(ContractViolation, match="invalid email verification evidence"):
        _check_user({"id": "source-user", "email_verification": "assumed"})
