"""Pure bounded ADF text; no attachments, expansion or URL fetching."""


class ADFError(ValueError):
    def __init__(self):
        super().__init__("jira_adf_invalid")


def text(value):
    if value is None:
        return ""
    if isinstance(value, str):
        if "\0" in value:
            raise ADFError()
        try:
            value.encode()
        except UnicodeError:
            raise ADFError() from None
        return value
    if not isinstance(value, dict) or value.get("type") != "doc" or value.get("version") != 1:
        raise ADFError()
    stack = [(value, 0)]
    out = []
    rendered = []
    count = 0
    size = 0
    while stack:
        node, depth = stack.pop()
        count += 1
        if (
            count > 100_000
            or depth > 64
            or not isinstance(node, dict)
            or not isinstance(node.get("type"), str)
        ):
            raise ADFError()
        kind = node["type"]
        if kind.startswith("media") or kind in ("inlineCard", "blockCard", "embedCard"):
            continue
        if kind == "text":
            out.append(text(node.get("text")))
        elif kind == "mention":
            attrs = node.get("attrs", {})
            if not isinstance(attrs, dict):
                raise ADFError()
            out.append("@" + text(attrs.get("text") or "user"))
        elif kind == "hardBreak" or kind == "_end":
            out.append("\n")
        else:
            children = node.get("content", [])
            if not isinstance(children, list):
                raise ADFError()
            if kind in ("paragraph", "heading", "listItem", "tableRow", "codeBlock"):
                stack.append(({"type": "_end"}, depth))
            if len(stack) + len(children) > 100_000:
                raise ADFError()
            stack.extend((child, depth + 1) for child in reversed(children))
        if out:
            part = out.pop()
            size += len(part.encode())
            rendered.append(part)
        # Accumulate text separately to avoid quadratic rescans.
        if size > 8 << 20:
            raise ADFError()
    return "".join(rendered).strip()
