# pyright: reportPrivateUsage=false
from __future__ import annotations

from ._indexing import Indexing
from ._lifecycle import Lifecycle
from ._preparation import Preparation
from ._runtime import NamespaceRuntime
from ._work import Published, Rebuilt
from .processing import _ProcessingStopped, _ProcessingYielded
from .types import DocumentId


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
        preferred: DocumentId | None = None
        while (
            permit := self.lifecycle.claim(
                self.runtime.bindings, self.runtime.index_errors, preferred
            )
        ) is not None:
            preferred = permit.identity
            try:
                if permit.payload["stage"] == "process" and not permit.payload.get("cleanup"):
                    result = self.preparation.execute(permit)
                else:
                    result = self.indexing.execute(permit)
                if result is not None and self.lifecycle.commit(permit, result):
                    if isinstance(result, Published):
                        self.preparation.release_text(permit.subject.revision)
                    elif isinstance(result, Rebuilt):
                        self.runtime.index_errors.discard(permit.identity.namespace)
            except (_ProcessingYielded, _ProcessingStopped):
                self.lifecycle.advance(permit.identity, permit.payload)
            except Exception as error:
                self.lifecycle.fail_job(permit.identity, permit.payload, error)
            finally:
                self.lifecycle.retire(permit)
