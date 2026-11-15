"""Provider config models: schemas with secret marks, value-free parsing."""

import pytest
from sample_provider import Scope, Settings

from fyrr_connectors import ConfigError, Secret
from fyrr_connectors.config import SECRET_MARK

SENTINEL = "SENTINEL-cfg-41d9"


def test_schema_marks_secrets_and_states_constraints() -> None:
    schema = Settings.json_schema()
    token = schema["properties"]["api_token"]
    assert token[SECRET_MARK] is True
    assert token["required"] == ["env"] and token["additionalProperties"] is False
    assert schema["properties"]["page_size"] == {
        "type": "integer",
        "description": "Documents per batch.",
        "minimum": 1,
        "maximum": 100,
        "default": 2,
    }
    assert schema["properties"]["mode"]["enum"] == ["fast", "careful"]
    assert schema["required"] == ["api_token"]
    assert schema["additionalProperties"] is False
    assert Scope.json_schema()["properties"]["titles"]["minItems"] == 1


def test_parse_takes_secrets_only_as_environment_references(monkeypatch) -> None:
    settings = Settings.parse({"api_token": {"env": "SAMPLE_TOKEN"}}, "settings")
    assert settings.api_token == Secret("SAMPLE_TOKEN")
    assert SENTINEL not in repr(settings)
    monkeypatch.setenv("SAMPLE_TOKEN", SENTINEL)
    assert settings.api_token.reveal() == SENTINEL
    assert SENTINEL not in repr(settings) and SENTINEL not in str(settings.api_token)
    for literal in [
        SENTINEL,
        {"env": "SAMPLE", "value": SENTINEL},
        {"value": SENTINEL},
        [SENTINEL],
    ]:
        with pytest.raises(ConfigError) as caught:
            Settings.parse({"api_token": literal}, "settings")
        assert caught.value.path == "settings.api_token"
        assert "never stored in the config file" in str(caught.value)
        assert SENTINEL not in str(caught.value)


@pytest.mark.parametrize(
    ("data", "path", "message"),
    [
        ({"api_token": {"env": "T"}, "page_size": 0}, "settings.page_size", "at least 1"),
        (
            {"api_token": {"env": "T"}, "page_size": SENTINEL},
            "settings.page_size",
            "expected an integer",
        ),
        (
            {"api_token": {"env": "T"}, "page_size": True},
            "settings.page_size",
            "expected an integer",
        ),
        (
            {"api_token": {"env": "T"}, "mode": SENTINEL},
            "settings.mode",
            "expected one of fast, careful",
        ),
        ({"api_token": {"env": "T"}, "extra": SENTINEL}, "settings.extra", "unknown field"),
        ({}, "settings.api_token", "missing field"),
    ],
)
def test_parse_errors_name_the_field_never_the_value(data, path: str, message: str) -> None:
    with pytest.raises(ConfigError) as caught:
        Settings.parse(data, "settings")
    assert caught.value.path == path
    assert message in caught.value.message
    assert SENTINEL not in str(caught.value)


def test_unset_secrets_name_the_variable(monkeypatch) -> None:
    monkeypatch.delenv("SAMPLE_UNSET", raising=False)
    with pytest.raises(ConfigError, match="SAMPLE_UNSET is not set"):
        Secret("SAMPLE_UNSET").reveal()
    with pytest.raises(ConfigError):
        Secret("lower-case")


def test_unsupported_field_types_fail_at_class_definition() -> None:
    from fyrr_connectors import ConfigModel

    with pytest.raises(TypeError, match="unsupported config field type"):

        class Broken(ConfigModel):
            mapping: dict[str, str]
