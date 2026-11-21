# fyrr-connectors

The Python connector SDK and bundled providers for
[fyrr](https://github.com/fyrr-ai/fyrr), permission-aware retrieval for AI
agents over MCP. A provider registers a `provider_info()` callable under the
`fyrr.connectors` entry point group. Its `Connector` subclass yields batches
for content, permissions, members and reconciliation; content uses
contract-exact Arrow batches built with `document_changes(...)`.

The bundled providers are `local-files`, `google-drive`, `notion`, `github`,
`slack` and `jira`. The cloud providers need their optional extras of the
same names (`fyrr-connectors[google-drive]`, …). The `testkit` extra
validates a provider's metadata, configuration and batches against the
schemas bundled in the wheel.

The `fyrr` binary contains no connectors and no Python: it starts a Python
provider host from the environment this package is installed in (set
`VIRTUAL_ENV` to it, or have its `python3` next to `fyrr` or on `PATH`). The
package needs Python 3.13 or later.

The package version is declared in `pyproject.toml`; read the installed
version with `importlib.metadata.version("fyrr-connectors")`. The SDK series
(`0.y` for `0.y.z`, `x` for `x.y.z` with `x > 0`) is the compatibility promise
for providers.

- [Writing a connector provider](https://github.com/fyrr-ai/fyrr/blob/main/docs/connectors/authoring.md)
- [Bundled connectors: setup, permissions and limits](https://github.com/fyrr-ai/fyrr/blob/main/docs/connectors/README.md)
- [Example provider](https://github.com/fyrr-ai/fyrr/tree/main/examples/markdown-provider)

From a repository checkout, `scripts/ci` verifies the SDK and the providers.
