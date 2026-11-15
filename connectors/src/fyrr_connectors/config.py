"""Frozen provider configuration models with JSON Schema and value-free errors.

Secret fields accept only { env = "VAR" } references. Secret.reveal() reads the
value when needed; generated schemas mark these fields with x-fyrr-secret.
"""

from __future__ import annotations

import dataclasses
import inspect
import os
import re
import types
import typing
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Literal, Self, get_args, get_origin, get_type_hints

ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")
#: Schema keyword marking a secret property.
SECRET_MARK = "x-fyrr-secret"
SECRET_IN_FILE = 'secrets are never stored in the config file; write { env = "VAR_NAME" } instead'
#: The schema of an environment reference, as in fyrr's own configuration.
SECRET_REF_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"env": {"type": "string", "pattern": ENV_NAME.pattern}},
    "required": ["env"],
    "additionalProperties": False,
}


class ConfigError(ValueError):
    """A configuration value does not match its model. Never quotes values."""

    def __init__(self, path: str, message: str) -> None:
        super().__init__(f"{path}: {message}")
        self.path = path
        self.message = message


class Secret:
    """A secret held in an environment variable; its value is never formatted."""

    __slots__ = ("env",)

    def __init__(self, env: str) -> None:
        if not isinstance(env, str) or not ENV_NAME.match(env):
            raise ConfigError("env", "must name an environment variable ([A-Z_][A-Z0-9_]*)")
        self.env = env

    def reveal(self) -> str:
        """Read the secret from the environment."""
        value = os.environ.get(self.env)
        if not value:
            raise ConfigError(f"env:{self.env}", f"environment variable {self.env} is not set")
        return value

    def __repr__(self) -> str:
        return f"Secret(env:{self.env})"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and other.env == self.env

    def __hash__(self) -> int:
        return hash(("Secret", self.env))


def setting(
    *,
    description: str,
    default: Any = dataclasses.MISSING,
    default_factory: Any = dataclasses.MISSING,
    pattern: str | None = None,
    minimum: float | None = None,
    maximum: float | None = None,
    min_items: int | None = None,
    max_items: int | None = None,
) -> Any:
    """Declare a field of a `ConfigModel` with its schema constraints."""
    constraints = {
        "description": description,
        "pattern": pattern,
        "minimum": minimum,
        "maximum": maximum,
        "minItems": min_items,
        "maxItems": max_items,
    }
    metadata = {"fyrr": {key: value for key, value in constraints.items() if value is not None}}
    return dataclasses.field(default=default, default_factory=default_factory, metadata=metadata)


def secret(*, description: str, required: bool = True) -> Any:
    """Declare a `Secret` field (type it `Secret`, or `Secret | None` if optional)."""
    if required:
        return setting(description=description)
    return setting(description=description, default=None)


@dataclasses.dataclass(frozen=True)
class _Field:
    name: str
    annotation: Any
    required: bool
    default: Any
    constraints: Mapping[str, Any]


class ConfigModel:
    """Base of `settings` and `scope` models.

    Subclasses are frozen keyword-only dataclasses; annotate fields with `str`,
    `int`, `float`, `bool`, `Secret`, `Literal[...]`, a nested `ConfigModel`,
    `list[...]`/`tuple[..., ...]` of those, or `X | None` (optional, default
    None). Unknown keys are refused.
    """

    __fyrr_fields__: ClassVar[tuple[_Field, ...]] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        dataclasses.dataclass(frozen=True, kw_only=True)(cls)
        hints = get_type_hints(cls)
        fields = []
        for field in dataclasses.fields(cls):  # type: ignore[arg-type]
            has_default = (
                field.default is not dataclasses.MISSING
                or field.default_factory is not dataclasses.MISSING
            )
            default = field.default
            if field.default_factory is not dataclasses.MISSING:
                default = field.default_factory()
            fields.append(
                _Field(
                    field.name,
                    hints[field.name],
                    not has_default,
                    default,
                    field.metadata.get("fyrr", {}),
                )
            )
        cls.__fyrr_fields__ = tuple(fields)
        # Fail at class definition, not at the first config check.
        cls.json_schema()

    @classmethod
    def json_schema(cls) -> dict[str, Any]:
        """The JSON Schema (draft 2020-12) of this model."""
        properties: dict[str, Any] = {}
        required = []
        for field in cls.__fyrr_fields__:
            schema = _type_schema(field.annotation, f"{cls.__qualname__}.{field.name}")
            schema.update(field.constraints)
            if field.required:
                required.append(field.name)
            elif field.default is not None and not _is_secret(field.annotation):
                schema["default"] = _json_default(field.default)
            properties[field.name] = schema
        result: dict[str, Any] = {"type": "object"}
        if cls.__doc__ and cls.__doc__ is not ConfigModel.__doc__:
            result["description"] = inspect.cleandoc(cls.__doc__)
        result["properties"] = properties
        if required:
            result["required"] = required
        result["additionalProperties"] = False
        return result

    @classmethod
    def parse(cls, data: Mapping[str, Any] | None, path: str = "") -> Self:
        """Build the model from a TOML table, refusing what the schema refuses."""
        if data is None:
            data = {}
        where = path or "<table>"
        if not isinstance(data, Mapping):
            raise ConfigError(where, "must be a table")
        names = {field.name for field in cls.__fyrr_fields__}
        for key in data:
            if key not in names:
                raise ConfigError(_join(path, str(key)), "unknown field")
        values = {}
        for field in cls.__fyrr_fields__:
            at = _join(path, field.name)
            if field.name not in data:
                if field.required:
                    raise ConfigError(at, "missing field")
                values[field.name] = field.default
                continue
            values[field.name] = _convert(field.annotation, data[field.name], at, field.constraints)
        return cls(**values)


class NoConfig(ConfigModel):
    """No fields."""


def _join(path: str, key: str) -> str:
    return f"{path}.{key}" if path else key


def _optional(annotation: Any) -> Any | None:
    """The inner type of `X | None`, else None."""
    if get_origin(annotation) in (typing.Union, types.UnionType):
        inner = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(inner) == 1 and len(get_args(annotation)) == 2:
            return inner[0]
    return None


def _is_secret(annotation: Any) -> bool:
    return annotation is Secret or _optional(annotation) is Secret


def _sequence_item(annotation: Any) -> Any | None:
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (list, Sequence) and len(args) == 1:
        return args[0]
    if origin is tuple and len(args) == 2 and args[1] is Ellipsis:
        return args[0]
    return None


def _type_schema(annotation: Any, where: str) -> dict[str, Any]:
    inner = _optional(annotation)
    if inner is not None:
        return _type_schema(inner, where)
    if annotation is Secret:
        return {SECRET_MARK: True, **SECRET_REF_SCHEMA}
    if annotation in (bool, int, float, str):
        return {
            "type": {bool: "boolean", int: "integer", float: "number", str: "string"}[annotation]
        }
    if get_origin(annotation) is Literal:
        options = list(get_args(annotation))
        if not all(isinstance(option, str) for option in options):
            raise TypeError(f"{where}: Literal options must be strings")
        return {"type": "string", "enum": options}
    item = _sequence_item(annotation)
    if item is not None:
        return {"type": "array", "items": _type_schema(item, f"{where}[]")}
    if isinstance(annotation, type) and issubclass(annotation, ConfigModel):
        return annotation.json_schema()
    raise TypeError(f"{where}: unsupported config field type {annotation!r}")


def _json_default(value: Any) -> Any:
    if isinstance(value, tuple | list):
        return [_json_default(item) for item in value]
    if isinstance(value, ConfigModel):
        return {
            field.name: _json_default(getattr(value, field.name)) for field in value.__fyrr_fields__
        }
    return value


def _convert(annotation: Any, value: Any, path: str, constraints: Mapping[str, Any]) -> Any:
    inner = _optional(annotation)
    if inner is not None:
        return _convert(inner, value, path, constraints)
    if annotation is Secret:
        if isinstance(value, Mapping) and set(value) == {"env"}:
            env = value["env"]
            if isinstance(env, str) and ENV_NAME.match(env):
                return Secret(env)
        raise ConfigError(path, SECRET_IN_FILE)
    if annotation in (bool, int, float, str):
        expected, description = {
            bool: (bool, "a boolean"),
            int: (int, "an integer"),
            float: (int | float, "a number"),
            str: (str, "a string"),
        }[annotation]
        numeric = annotation in (int, float)
        if not isinstance(value, expected) or (numeric and isinstance(value, bool)):
            raise ConfigError(path, f"expected {description}")
        if numeric:
            low, high = constraints.get("minimum"), constraints.get("maximum")
            if low is not None and value < low:
                raise ConfigError(path, f"must be at least {low}")
            if high is not None and value > high:
                raise ConfigError(path, f"must be at most {high}")
        pattern = constraints.get("pattern") if annotation is str else None
        if pattern is not None and not re.search(pattern, value):
            raise ConfigError(path, f"does not match the pattern {pattern}")
        return float(value) if annotation is float else value
    if get_origin(annotation) is Literal:
        options = get_args(annotation)
        if value not in options:
            raise ConfigError(path, "expected one of " + ", ".join(map(str, options)))
        return value
    item = _sequence_item(annotation)
    if item is not None:
        if not isinstance(value, list | tuple):
            raise ConfigError(path, "expected an array")
        low, high = constraints.get("minItems"), constraints.get("maxItems")
        if low is not None and len(value) < low:
            raise ConfigError(path, f"needs at least {low} entries")
        if high is not None and len(value) > high:
            raise ConfigError(path, f"allows at most {high} entries")
        return tuple(
            _convert(item, entry, f"{path}[{index}]", {}) for index, entry in enumerate(value)
        )
    if isinstance(annotation, type) and issubclass(annotation, ConfigModel):
        return annotation.parse(value, path)
    raise TypeError(f"{path}: unsupported config field type {annotation!r}")
