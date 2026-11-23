"""A local `file://` deployment for the whole session, started by examples/guides/start."""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
# The guides' start script exports these. The script then waits for the end of its standard
# input, and stops fyrr and removes its directory.
START = (
    "source examples/guides/start || exit 1; "
    'printf "%s\\n" "$READER" "$WRITER" "$ADMIN_KEY" "$READ_KEY"; '
    "read -r _; fyrr_stop"
)


@dataclass(frozen=True)
class Deployment:
    reader_url: str
    writer_url: str
    admin_key: str
    read_key: str


@pytest.fixture(scope="session")
def deployment() -> Iterator[Deployment]:
    if shutil.which("fyrr") is None:
        pytest.fail("no `fyrr` on PATH: build it, or run `scripts/ci --step='python client'`")
    process = subprocess.Popen(
        ["bash", "-c", START],
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    values = [process.stdout.readline().strip() for _ in range(4)]
    try:
        if not all(values):
            pytest.fail(f"fyrr did not start (exit {process.poll()}); see its log above")
        yield Deployment(*values)
    finally:
        if process.stdin:
            process.stdin.close()
        process.wait(timeout=60)
