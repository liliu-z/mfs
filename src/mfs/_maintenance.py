from __future__ import annotations

import time

from ._cleanup import IndexCleanup
from ._configuration import Configuration
from ._lifecycle import Lifecycle
from .errors import StorageFailed
from .types import DocumentId


class Maintenance:
    """A bounded set of owners; one namespace's management never runs twice at once."""

    def __init__(
        self, lifecycle: Lifecycle, configuration: Configuration, cleanup: IndexCleanup
    ) -> None:
        self.lifecycle, self.configuration, self.cleanup = lifecycle, configuration, cleanup
        self.active: set[str] = set()
        self.last: dict[str, int] = {}
        self.next_run: dict[str, float] = {}
        self.sequence = 0

    def run(self) -> None:
        try:
            self._run()
        except Exception as error:
            with self.lifecycle.condition:
                self.lifecycle.storage_error = (
                    error
                    if isinstance(error, StorageFailed)
                    else StorageFailed(f"maintenance could not persist: {error}")
                )
                self.lifecycle.stop()

    def _run(self) -> None:
        lifecycle = self.lifecycle
        while True:
            with lifecycle.condition:
                if lifecycle.stopping:
                    return
                if lifecycle.boot_paused:
                    lifecycle.condition.wait(0.05)
                    continue
                now = time.time()
                dirty = {
                    row[0]
                    for row in lifecycle.catalog.query(
                        "SELECT DISTINCT json_extract(value,'$.namespace') FROM index_cleanup "
                        "WHERE json_extract(value,'$.failures')<5 "
                        "AND json_extract(value,'$.next_run')<=?",
                        (now,),
                    )
                }
                self.last = {
                    n: order for n, order in self.last.items() if n in lifecycle.namespaces
                }
                self.next_run = {n: due for n, due in self.next_run.items() if n in self.last}
                candidates = [
                    n
                    for n, record in lifecycle.namespaces.items()
                    if n not in self.active
                    and self.next_run.get(n, 0) <= time.monotonic()
                    and not lifecycle.held(DocumentId(n, ""), record)
                    and (n in dirty or record.get("building") or record.get("retiring_generations"))
                ]
                if not candidates:
                    lifecycle.condition.wait(0.05)
                    continue
                namespace = min(candidates, key=lambda n: self.last.get(n, -1))
                incarnation = lifecycle.namespaces[namespace]["incarnation"]
                self.sequence += 1
                self.last[namespace] = self.sequence
                self.active.add(namespace)
                lifecycle.namespace_executions[incarnation] = (
                    lifecycle.namespace_executions.get(incarnation, 0) + 1
                )
            try:
                self.cleanup.maintain(namespace)
                self.configuration.maintain(namespace)
            finally:
                with lifecycle.condition:
                    self.active.discard(namespace)
                    self.next_run[namespace] = time.monotonic() + 0.05
                    lifecycle.namespace_executions[incarnation] -= 1
                    lifecycle.condition.notify_all()
            with lifecycle.condition:
                lifecycle.condition.wait(0.05)
