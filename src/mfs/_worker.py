# pyright: reportPrivateUsage=false
from __future__ import annotations

from ._indexing import Indexing
from ._lifecycle import Lifecycle
from ._preparation import Preparation
from ._runtime import NamespaceRuntime
from ._work import Published, Rebuilt, StepResult


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
        while (
            permit := self.lifecycle.claim(self.runtime.bindings, self.runtime.index_errors)
        ) is not None:
            result: StepResult | None = None
            error: BaseException | None = None
            try:
                if permit.payload["stage"] == "process" and not permit.payload.get("cleanup"):
                    result = self.preparation.execute(permit)
                else:
                    result = self.indexing.execute(permit)
            except BaseException as caught:
                error = caught
            if self.lifecycle.finish_execution(permit, result, error):
                if isinstance(result, Published):
                    self.preparation.release_text(permit.subject.revision)
                elif isinstance(result, Rebuilt):
                    self.runtime.index_errors.discard(permit.identity.namespace)
