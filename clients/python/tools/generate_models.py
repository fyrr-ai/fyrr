"""Generate src/fyrr_client/models.py from schemas/openapi.json.

    uv run python tools/generate_models.py          # rewrite models.py
    uv run python tools/generate_models.py --check  # fail when models.py is stale

The output is typing.TypedDict classes, so a request is a plain dict and a response is the
decoded JSON. A schema member that is a keyword in Python ("and", "from") makes the class use the
functional TypedDict syntax.
"""

from __future__ import annotations

import json
import keyword
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent.parent
SPEC = HERE.parent.parent / "schemas" / "openapi.json"
OUTPUT = HERE / "src" / "fyrr_client" / "models.py"
PREFIX = "#/components/schemas/"
SCALARS = {"string": "str", "integer": "int", "number": "float", "boolean": "bool"}

HEADER = '''\
"""Typed requests and responses of the fyrr HTTP API.

Generated from schemas/openapi.json by tools/generate_models.py. Do not edit.
"""

from __future__ import annotations

from typing import Any, Literal, NotRequired, TypeAlias, TypedDict
'''


class Generator:
    def __init__(self, schemas: dict[str, Any]) -> None:
        self.schemas = schemas
        self.classes: dict[str, str] = {}
        self.aliases: dict[str, str] = {}
        self.alias_uses: dict[str, set[str]] = {}

    def run(self) -> str:
        for name, schema in self.schemas.items():
            self.define(name, schema)
        parts = [HEADER.rstrip(), *self.classes.values()]
        parts += [self.aliases[name] for name in self.alias_order()]
        return "\n\n\n".join(parts).rstrip() + "\n"

    def alias_order(self) -> list[str]:
        """Aliases whose right side names another alias come after it."""
        done: list[str] = []

        def visit(name: str) -> None:
            if name in done:
                return
            for used in sorted(self.alias_uses[name] & self.aliases.keys()):
                if used != name:
                    visit(used)
            done.append(name)

        for name in self.aliases:
            visit(name)
        return done

    def define(self, name: str, schema: dict[str, Any]) -> None:
        if is_object(schema):
            self.define_class(name, schema)
            return
        expression = self.expression(schema, name)
        comment = f"# {schema['description'].splitlines()[0]}\n" if "description" in schema else ""
        self.aliases[name] = f"{comment}{name}: TypeAlias = {expression}"
        self.alias_uses[name] = set(identifiers(expression))

    def define_class(self, name: str, schema: dict[str, Any]) -> None:
        required = set(schema.get("required", []))
        fields: list[tuple[str, str, str]] = []
        for key, value in schema["properties"].items():
            kind = self.expression(value, name + camel(key))
            description = value.get("description", "") if isinstance(value, dict) else ""
            fields.append((key, kind if key in required else f"NotRequired[{kind}]", description))
        doc = docstring(schema.get("description", ""), "    ")
        if all(key.isidentifier() and not keyword.iskeyword(key) for key, _, _ in fields):
            lines = [f"class {name}(TypedDict):"]
            if doc:
                lines.append(doc)
            for key, kind, description in fields:
                lines.append(f"    {key}: {kind}")
                if description:
                    lines.append(docstring(description, "    "))
            self.classes[name] = "\n".join(lines if fields or doc else [*lines, "    pass"])
        else:
            body = ", ".join(f'"{key}": "{kind}"' for key, kind, _ in fields)
            self.classes[name] = f'{name} = TypedDict("{name}", {{{body}}})'

    def expression(self, schema: Any, context: str) -> str:
        """The Python type of a schema; an inline object becomes a class named after `context`."""
        if schema is True or schema == {}:
            return "Any"
        if "$ref" in schema:
            return str(schema["$ref"]).removeprefix(PREFIX)
        for key in ("anyOf", "oneOf"):
            if key in schema:
                return self.union(schema[key], context)
        if "const" in schema:
            return f"Literal[{json.dumps(schema['const'])}]"
        if "enum" in schema:
            return "Literal[" + ", ".join(json.dumps(item) for item in schema["enum"]) + "]"
        kinds = schema.get("type")
        kinds = kinds if isinstance(kinds, list) else [kinds]
        base = [kind for kind in kinds if kind != "null"]
        assert len(base) == 1, f"unsupported schema: {schema}"
        kind = base[0]
        if kind == "array":
            item = (
                "Any"
                if "items" not in schema
                else self.expression(schema["items"], context + "Item")
            )
            result = f"list[{item}]"
        elif kind == "object":
            result = self.object_type(schema, context)
        else:
            result = SCALARS[kind]
        return f"{result} | None" if "null" in kinds else result

    def object_type(self, schema: dict[str, Any], context: str) -> str:
        if is_object(schema):
            self.define_class(context, schema)
            return context
        extra = schema.get("additionalProperties", True)
        return (
            "dict[str, Any]" if extra is True else f"dict[str, {self.expression(extra, context)}]"
        )

    def union(self, members: list[Any], context: str) -> str:
        literals: list[str] = []
        others: list[str] = []
        for member in members:
            if member.get("type") == "null":
                text = "None"
            elif is_object(member):
                text = self.inline_object(member, context)
            else:
                text = self.expression(member, context)
            if text.startswith("Literal["):
                literals.extend(text.removeprefix("Literal[").removesuffix("]").split(", "))
            elif text not in others:
                others.append(text)
        merged = [f"Literal[{', '.join(literals)}]"] if literals else []
        return " | ".join(sorted(merged + others, key=lambda text: text == "None"))

    def inline_object(self, member: dict[str, Any], context: str) -> str:
        first = next(iter(member["properties"]))
        tag = member["properties"][first].get("const", first)
        name = context + camel(str(tag))
        assert name not in self.classes, f"inline class name collides: {name}"
        self.define_class(name, member)
        return name


def is_object(schema: dict[str, Any]) -> bool:
    """An object with known members and no others: a TypedDict."""
    return (
        schema.get("type") == "object"
        and "properties" in schema
        and schema.get("additionalProperties", False) is False
    )


def camel(text: str) -> str:
    return "".join(word.capitalize() for word in text.replace("-", "_").split("_"))


def identifiers(expression: str) -> list[str]:
    word = ""
    found = []
    for character in expression + " ":
        if character.isalnum() or character == "_":
            word += character
        elif word:
            found.append(word)
            word = ""
    return found


def docstring(text: str, indent: str) -> str:
    if not text:
        return ""
    body = text.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
    lines = [f"{indent}{line}".rstrip() for line in body.splitlines()]
    if len(lines) == 1:
        return f'{indent}"""{body}"""'
    return f'{indent}"""' + lines[0].lstrip() + "\n" + "\n".join(lines[1:]) + f'\n{indent}"""'


def generate() -> str:
    spec = json.loads(SPEC.read_text())
    return Generator(spec["components"]["schemas"]).run()


def main() -> int:
    text = generate()
    if "--check" in sys.argv[1:]:
        if OUTPUT.read_text() != text:
            print(
                f"{OUTPUT} is stale: run `uv run python tools/generate_models.py`", file=sys.stderr
            )
            return 1
        return 0
    OUTPUT.write_text(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
