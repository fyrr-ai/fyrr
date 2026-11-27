"""What the bench scripts share: loopback ports, the timed HTTP call,
nearest-rank percentiles and the machine a run measured on."""

import contextlib
import http.client
import json
import math
import os
import platform
import socket
import time
from pathlib import Path


def free_address():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return "127.0.0.1:%d" % probe.getsockname()[1]


def percentile(ordered, fraction):
    """Nearest rank of sorted values."""
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def machine():
    cpu = next(
        (
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        ),
        "unknown",
    )
    memory = next(
        int(line.split()[1]) // 1024
        for line in Path("/proc/meminfo").read_text().splitlines()
        if line.startswith("MemTotal")
    )
    return {
        "kernel": platform.release(),
        "cpu": cpu,
        "logical_cpus": os.cpu_count(),
        "memory_mib": memory,
    }


class Server:
    def __init__(self, address):
        self.host, port = address.split(":")
        self.port = int(port)

    def call(self, method, path, key, body=None, timeout=1800):
        """One request on its own connection. The clock starts when the
        connection is made, so the connection is not in the time."""
        headers = {"Authorization": "Bearer " + key, "Content-Type": "application/json"}
        connection = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        connection.connect()
        started = time.perf_counter()
        try:
            connection.request(method, path, body, headers)
        except (BrokenPipeError, ConnectionResetError):
            pass  # a refusal can answer before the body is sent; the answer is read
        response = connection.getresponse()
        answer = response.read()
        elapsed = time.perf_counter() - started
        connection.close()
        with contextlib.suppress(ValueError):
            answer = json.loads(answer)
        return response.status, answer, elapsed
