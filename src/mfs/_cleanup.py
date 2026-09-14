from __future__ import annotations

import time
from typing import Any

from ._json import compact_json
from ._lifecycle import Lifecycle
from ._runtime import NamespaceRuntime
from .types import DocumentId


class IndexCleanup:
    """Retire exact publications independently of newer source processing."""

    def __init__(self, lifecycle: Lifecycle, runtime: NamespaceRuntime) -> None:
        self.lifecycle, self.runtime = lifecycle, runtime
        self.catalog = lifecycle.catalog

    def timed_out(self, key: str, debt: dict[str, Any]) -> None:
        debt.update(failures=5, next_run=0, error="index cleanup exceeded its stage timeout")
        with self.lifecycle.state_transaction():
            self.catalog.execute(
                "UPDATE index_cleanup SET value=? WHERE key=?", (compact_json(debt), key)
            )

    def maintain(self, namespace: str) -> None:
        with self.lifecycle.condition:
            rows = self.catalog.cleanup_rows(namespace)
        processed = 0
        for key, debt in rows:
            if processed >= 32:
                break
            if debt["failures"] >= 5 or debt["next_run"] > time.time():
                continue
            with self.lifecycle.condition:
                if self.lifecycle.stopping:
                    return
                drop = self.lifecycle.targets.get(DocumentId(namespace, ""), {})
                if drop.get("kind") == "drop" and debt["incarnation"] in drop.get(
                    "incarnations", []
                ):
                    # The collection-level owner settles these debts atomically.
                    continue
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
                with self.runtime.maintenance(lambda key=key, debt=debt: self.timed_out(key, debt)):
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
                debt.update(failures=min(5, debt["failures"] + 1), error=str(error))
                debt["next_run"] = time.time() + min(30, 0.25 * 2 ** debt["failures"])
                with self.lifecycle.condition, self.catalog.transaction():
                    self.catalog.execute(
                        "UPDATE index_cleanup SET value=? WHERE key=?", (compact_json(debt), key)
                    )
                continue
            with self.lifecycle.condition, self.catalog.transaction():
                self.catalog.execute("DELETE FROM index_cleanup WHERE key=?", (key,))
                self.lifecycle.condition.notify_all()
