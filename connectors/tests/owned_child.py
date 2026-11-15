"""Own a piped child and reap it even when a transport assertion fails."""

import subprocess
from contextlib import contextmanager


@contextmanager
def owned_child(command):
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        yield process
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(10)
        process.stdin.close()
        process.stdout.close()
