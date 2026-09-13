# pyright: reportPrivateUsage=false
from __future__ import annotations

from ._indexing import Indexing
from ._lifecycle import Lifecycle
from ._preparation import Preparation
from ._runtime import NamespaceRuntime
from ._work import ExecutionPermit, Published, Rebuilt, StepResult
from .errors import StorageFailed


class Worker:
    """One execution loop; task ownership, commits and recovery belong to Lifecycle."""

    def __init__(
        self,
        lifecycle: Lifecycle,
        runtime: NamespaceRuntime,
        preparation: Preparation,
        indexing: Indexing,
    ) -> None:
        self.lifecycle, self.runtime = lifecycle, runtime
        self.preparation, self.indexing = preparation, indexing

    def run(self) -> None:
        while True:
            try:
                permit = self.lifecycle.claim(
                    self.runtime.bindings, self.runtime.index_errors, self.runtime.acquire_stage
                )
            except Exception as error:
                with self.lifecycle.condition:
                    self.lifecycle.storage_error = StorageFailed(
                        f"scheduler could not persist: {error}"
                    )
                    self.lifecycle.stop()
                return
            if permit is None:
                return
            with self.preparation.artifacts.operation():
                self.execute(permit)
            del permit  # An idle worker must not retain the previous namespace's adapters.

    def execute(self, permit: ExecutionPermit) -> None:
        result: StepResult | None = None
        error: BaseException | None = None
        try:
            if permit.payload["stage"] == "process" and not permit.payload.get("cleanup"):
                result = self.preparation.execute(permit)
            else:
                result = self.indexing.execute(permit)
        except BaseException as caught:
            error = caught
        with self.lifecycle.condition:
            if self.lifecycle.finish_execution(permit, result, error):
                if isinstance(result, Published):
                    self.preparation.release_text(permit.subject.revision)
                elif isinstance(result, Rebuilt):
                    self.runtime.index_errors.discard(permit.identity.namespace)
            current = self.lifecycle.work_target(permit.identity, permit.payload)
            if (
                current is None
                or current["revision"] != permit.subject.revision
                or current["state"] in ("succeeded", "cancelled", "failed", "blocked")
            ):
                self.preparation.release_text(permit.subject.revision)
