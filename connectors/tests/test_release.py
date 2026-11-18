"""Exercise the workflow's shell with local GitHub and registry failure boundaries."""

import gzip
import hashlib
import os
import subprocess
import textwrap
from pathlib import Path

import pytest

WORKFLOW = Path(__file__).parents[2] / ".github/workflows/release.yml"
STEP = WORKFLOW.read_text().split("      - name: Publish the release and images\n", 1)[1]
PUBLICATION = textwrap.dedent(STEP.split("        run: |\n", 1)[1].split("\n      - name:", 1)[0])
PLATFORMS = {"linux-x86_64": "amd64", "linux-aarch64": "arm64"}
MANIFEST = "".join(f"platform {platform} manifest-{platform}.txt\n" for platform in PLATFORMS)
BOUNDARIES = r"""
set -euo pipefail
record() { printf '%s\n' "$*" >> events; }
gh() {
  record "gh:$*"
  case "$2" in
    view)
      if [[ "$FAILURE" == published-warning ]]; then echo warning >&2; fi
      if [[ "$FAILURE" == view ]]; then echo 'authentication failed' >&2; return 1; fi
      if [[ -f draft ]]; then cat draft; else echo 'release not found' >&2; return 1; fi ;;
    create) printf true > draft ;;
    upload) [[ "$FAILURE" != upload ]] ;;
    edit)
      [[ "$FAILURE" != publish ]] || return 1
      printf false > draft ;;
    *) return 2 ;;
  esac
}
docker() {
  record "docker:$*"
  if [[ "$1" == login ]]; then cat >/dev/null; fi
  if [[ "$1" == image && "$2" == load ]]; then cat >/dev/null; fi
  if [[ "$1" == image && "$2" == push && "$3" == *"/$FAILURE:0.1.0-arm64" ]]; then
    return 1
  fi
}
"""


@pytest.mark.parametrize(
    "failure", ["view", "upload", "fyrr", "fyrr-reader", "publish", "checksum", "published-warning"]
)
def test_release_retry_finishes_before_publication(tmp_path, failure):
    directory = tmp_path / "dist/fyrr-0.1.0"
    directory.mkdir(parents=True)
    (directory / "manifest.txt").write_text(MANIFEST)
    for platform in PLATFORMS:
        (directory / f"manifest-{platform}.txt").write_text("image_tag wt-0123456789ab\n")
        for image in ("fyrr", "fyrr-reader"):
            archive = directory / f"{image}-image-0.1.0-{platform}.tar.gz"
            archive.write_bytes(gzip.compress(b"image"))
    (directory / "SHA256SUMS").write_text(
        "".join(
            f"{hashlib.sha256(file.read_bytes()).hexdigest()}  {file.name}\n"
            for file in sorted(directory.iterdir())
        )
    )
    if failure == "checksum":
        (directory / "manifest.txt").write_text("corrupted")
    if failure == "published-warning":
        (tmp_path / "draft").write_text("false")
    env = dict(
        os.environ,
        TAG="v0.1.0",
        GH_TOKEN="test-token",
        ACTOR="test-user",
        GITHUB_REPOSITORY="example/fyrr",
        GITHUB_REPOSITORY_OWNER="Example",
        FAILURE=failure,
    )

    def run():
        return subprocess.run(
            ["bash", "-c", BOUNDARIES + PUBLICATION],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
        )

    interrupted = run()
    if failure == "published-warning":
        assert interrupted.returncode == 0, interrupted.stderr
        assert (tmp_path / "events").read_text().splitlines() == [
            "gh:release view v0.1.0 --json isDraft --jq .isDraft"
        ]
        return
    assert interrupted.returncode != 0, interrupted.stderr
    if failure == "checksum":
        assert not (tmp_path / "draft").exists()
        assert not (tmp_path / "events").exists()
        (directory / "manifest.txt").write_text(MANIFEST)
    elif failure == "view":
        assert not (tmp_path / "draft").exists()
        assert "authentication failed" in interrupted.stderr
    else:
        assert (tmp_path / "draft").read_text() == "true"
    if failure not in ("publish", "checksum"):
        assert "gh:release edit" not in (tmp_path / "events").read_text()

    env["FAILURE"] = "none"
    completed = run()
    assert completed.returncode == 0, completed.stderr
    assert (tmp_path / "draft").read_text() == "false"
    events = (tmp_path / "events").read_text().splitlines()
    assert sum(line.startswith("gh:release create") for line in events) == 1
    published = max(i for i, line in enumerate(events) if line.startswith("gh:release edit"))
    for image in ("fyrr", "fyrr-reader"):
        name = f"ghcr.io/example/{image}:0.1.0"
        join = f"docker:buildx imagetools create --tag {name} {name}-amd64 {name}-arm64"
        joined = events.index(join)
        assert joined < published
        for arch in PLATFORMS.values():
            # Every architecture is pushed before the manifest that joins them.
            pushed = max(
                i for i, line in enumerate(events) if line == f"docker:image push {name}-{arch}"
            )
            assert pushed < max(i for i, line in enumerate(events) if line == join)
            assert f"docker:image tag {image}:wt-0123456789ab {name}-{arch}" in events
