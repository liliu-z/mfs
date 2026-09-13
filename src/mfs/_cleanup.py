from __future__ import annotations

import time

from ._json import compact_json
from ._lifecycle import Lifecycle
from ._runtime import NamespaceRuntime
from .types import DocumentId


class IndexCleanup:
    """Retire exact publications independently of newer source processing."""

    def __init__(self, lifecycle: Lifecycle, runtime: NamespaceRuntime) -> None:
        self.lifecycle, self.runtime = lifecycle, runtime
        self.catalog = lifecycle.catalog

    def maintain(self) -> None:
        with self.lifecycle.condition:
            rows = self.catalog.cleanup_rows()
        processed = 0
        for key, debt in rows:
            if processed >= 32:
                break
            if debt["failures"] >= 5 or debt["next_run"] > time.time():
                continue
            with self.lifecycle.condition:
                if self.lifecycle.stopping:
                    return
                if self.lifecycle.generation_queries.get((debt["incarnation"], debt["generation"])):
                    continue
                if any(
                    j.get("incarnation") == debt["incarnation"]
                    and j.get("collection_generation") == debt["generation"]
                    and j.get("snapshot_id") == debt["snapshot"]
                    for j in self.lifecycle.execution_records.values()
                ):
                    continue
            try:
                processed += 1
                index = self.runtime.index(
                    debt["namespace"], debt["incarnation"], debt["generation"]
                )
                if index.client.has_collection(index.collection_name):
                    index.delete_snapshot(
                        DocumentId(debt["namespace"], debt["doc_id"]),
                        debt["snapshot"],
                        debt["incarnation"],
                    )
            except Exception as error:
                debt.update(failures=debt["failures"] + 1, error=str(error))
                debt["next_run"] = time.time() + min(30, 0.25 * 2 ** debt["failures"])
                with self.lifecycle.condition, self.catalog.transaction():
                    self.catalog.execute(
                        "UPDATE index_cleanup SET value=? WHERE key=?", (compact_json(debt), key)
                    )
                continue
            with self.lifecycle.condition, self.catalog.transaction():
                self.catalog.execute("DELETE FROM index_cleanup WHERE key=?", (key,))
                self.lifecycle.condition.notify_all()
