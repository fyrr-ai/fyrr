# Shell functions the verify-fyrr recipes use. Source this file (bash or zsh)
# in every shell that runs a recipe, from anywhere inside the checkout:
#
#   . "$(git rev-parse --show-toplevel)/.claude/skills/verify-fyrr/env.sh"

# The helper, by the name the recipes use.
control-fyrr() {
  "$(git rev-parse --show-toplevel)/.claude/skills/verify-fyrr/control-fyrr" "$@"
}

# The documents of a search result, one `source_id/root_id` per line.
docs() { jq -r '.hits[] | "\(.source_id)/\(.root_id)"'; }

# The text of every record of a fetch result, as stored (it ends in a newline).
texts() { jq -j '.documents[].records[].text'; }

# The tool result inside the JSON-RPC answer of `POST /mcp`.
mcp_result() { jq -c '.result.structuredContent'; }
