from __future__ import annotations

import math
import struct
import time
from collections.abc import Mapping
from typing import Any

import blake3

from ._catalog import Catalog
from ._json import canonical_json


class VectorCache:
    """Bounded, disposable complete computations, independent of search visibility.

    Callers hold the Lifecycle lock across eligibility checks and cache writes.
    The byte budget covers stored keys, vectors and fixed row metadata; SQLite's
    page/WAL overhead is separate. Eviction never affects published index rows.
    """

    MAX_BYTES = 32 * 1024 * 1024

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    @staticmethod
    def prefix(job: dict[str, Any], dense: dict[str, Any]) -> str:
        return (
            blake3.blake3(
                canonical_json(
                    dict(incarnation=job["incarnation"], epoch=job["index_epoch"], dense=dense)
                )
            ).hexdigest()
            + ":"
        )

    def get(self, prefix: str, hashes: list[str], dimension: int) -> dict[str, list[float]]:
        result: dict[str, list[float]] = {}
        with self.catalog.transaction():
            for text_hash in hashes:
                key = prefix + text_hash
                row = self.catalog.one("SELECT vector,digest FROM vector_cache WHERE key=?", (key,))
                if row is None:
                    continue
                data = bytes(row[0])
                valid = len(data) == dimension * 8 and blake3.blake3(data).hexdigest() == row[1]
                vector = list(struct.unpack(f"<{dimension}d", data)) if valid else []
                if not valid or not all(math.isfinite(value) for value in vector):
                    self.catalog.execute("DELETE FROM vector_cache WHERE key=?", (key,))
                    continue
                result[text_hash] = vector
                self.catalog.execute(
                    "UPDATE vector_cache SET touched=? WHERE key=?", (time.time_ns(), key)
                )
        return result

    def put(self, prefix: str, incarnation: str, vectors: Mapping[str, list[float]]) -> None:
        with self.catalog.transaction():
            for text_hash, vector in vectors.items():
                data = struct.pack(f"<{len(vector)}d", *vector)
                self.catalog.execute(
                    "INSERT INTO vector_cache VALUES(?,?,?,?,?) ON CONFLICT(key) "
                    "DO UPDATE SET vector=excluded.vector,digest=excluded.digest,"
                    "touched=excluded.touched",
                    (
                        prefix + text_hash,
                        incarnation,
                        data,
                        blake3.blake3(data).hexdigest(),
                        time.time_ns(),
                    ),
                )
            # Newest computations win. Also evict an individual oversize entry.
            total = 0
            for key, size in self.catalog.query(
                "SELECT key,length(key)+length(incarnation)+length(vector)+length(digest)+8 "
                "FROM vector_cache ORDER BY touched DESC,key DESC"
            ):
                total += int(size)
                if total > self.MAX_BYTES:
                    self.catalog.execute("DELETE FROM vector_cache WHERE key=?", (key,))
