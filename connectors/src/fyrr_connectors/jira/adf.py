"""Pure bounded ADF text; no attachments, expansion or URL fetching."""


class ADFError(ValueError):
    def __init__(self):
        super().__init__("jira_adf_invalid")


def link(value):
    if isinstance(value, str) and value.lower().startswith(("http://", "https://")):
        return text(value)
    return None


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
        if kind.startswith("media"):
            continue
        if kind in ("inlineCard", "blockCard", "embedCard"):
            # A smart link is a pasted URL; one without a plain web URL stays dropped.
            url = link(node.get("attrs", {}).get("url")) or ""
            # Block and embed cards stand between paragraphs.
            out.append(url if kind == "inlineCard" or not url else url + "\n")
        elif kind == "text":
            body = text(node.get("text"))
            for mark in node.get("marks", ()):
                href = mark.get("type") == "link" and link(mark.get("attrs", {}).get("href"))
                if href:
                    body = body if body == href else f"{body} ({href})"
                    break
            out.append(body)
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
