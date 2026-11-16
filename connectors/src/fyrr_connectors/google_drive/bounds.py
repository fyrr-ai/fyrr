"""Aggregate acquisition limits; overflow aborts a complete snapshot."""

import json

MAX_ENTRIES = 200_000
MAX_EDGES = 1_000_000
MAX_BYTES = 64 << 20


class Budget:
    def __init__(self) -> None:
        self.entries = self.edges = self.bytes = 0

    def add(self, value: object, *, edges: int = 0) -> None:
        self.entries += 1
        self.edges += edges
        self.bytes += len(json.dumps(value, ensure_ascii=True).encode())
        if self.entries > MAX_ENTRIES or self.edges > MAX_EDGES or self.bytes > MAX_BYTES:
            raise ValueError("google_drive_aggregate_limit")
