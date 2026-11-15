# fyrr-provider-markdown (example)

An example connector provider built outside fyrr's workspace. It
registers `markdown-dir` under the `fyrr.connectors` entry point group and
syncs a directory of Markdown files as a fixed connection: content (one
document per file, a section per heading, optional `key: value` front matter
for title, author and labels) and a complete full-ID listing.

```sh
uv build --wheel                  # dist/fyrr_provider_markdown-0.1.0-py3-none-any.whl
uv run pytest                     # the SDK's provider test kit against tests/golden/
```

Install the wheel into a derived image (`FROM fyrr`, `pip install …whl`) or
mount a wheel directory (`FYRR_PROVIDER_WHEELS`); see
[providers in the image](https://github.com/fyrr-ai/fyrr/blob/main/docs/deployment.md#providers-in-the-image)
and the [connector author guide](https://github.com/fyrr-ai/fyrr/blob/main/docs/connectors/authoring.md).
