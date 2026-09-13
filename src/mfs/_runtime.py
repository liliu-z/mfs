from __future__ import annotations

import copy
import math
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ._index import ChunkIndex
from ._lifecycle import Lifecycle
from ._namespace import NamespaceBinding
from ._search_execution import SearchDeadline
from .errors import (
    CapabilityUnavailable,
    Closed,
    EmbeddingFailed,
    IndexUnavailable,
    InvalidConfiguration,
    MFSError,
    NamespaceCompatibilityError,
)
from .execution import Admission, ExecutionPolicy, LocalAdmission, ResourceGrant, ResourceLease
from .types import Chunker, ChunkRange, Embedder, Processor, SourceMap


class NamespaceRuntime:
    """Own namespace adapter bindings and collection routing, including model validation."""

    def __init__(
        self,
        path: Path,
        lifecycle: Lifecycle,
        policy: ExecutionPolicy | None = None,
        admission: Admission | None = None,
    ) -> None:
        self.lifecycle = lifecycle
        self.bindings: dict[str, NamespaceBinding] = {}
        self.build_bindings: dict[tuple[str, str], NamespaceBinding] = {}
        lifecycle.generation_bindings = self.build_bindings
        lifecycle.state_reconciled = self.reconcile_bindings
        self.collections: dict[str, ChunkIndex] = {}
        self.index_errors: set[str] = set()
        self.legacy_index = ChunkIndex(path / "milvus.db")
        self.admission = admission or LocalAdmission((policy or ExecutionPolicy()).resources)
        self._adapter_used: dict[int, int] = {}

    def acquire_adapter(self, adapter: object | None) -> ResourceLease | None:
        """Called under Lifecycle.condition; neither adapter nor host may block here."""
        capacity = type(adapter).__dict__.get("concurrency", 1)
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise InvalidConfiguration("adapter concurrency must be a positive integer")
        key = id(adapter)
        if adapter is not None and self._adapter_used.get(key, 0) >= capacity:
            return None
        resources = getattr(
            adapter, "resources", {getattr(adapter, "workload", "heavy"): 1} if adapter else {}
        )
        lease = self.admission.try_acquire(resources)
        if lease is None:
            return None
        if adapter is not None:
            self._adapter_used[key] = self._adapter_used.get(key, 0) + 1

        def release() -> None:
            try:
                lease.release()
            finally:
                with self.lifecycle.condition:
                    if adapter is not None:
                        self._adapter_used[key] -= 1
                    self.lifecycle.condition.notify_all()

        return ResourceGrant(release)

    def acquire_stage(self, identity: object, job: dict[str, Any]) -> ResourceLease | None:
        namespace = job["identity"]["namespace"]
        binding = (
            self.build_bindings.get((namespace, job["build_generation"]))
            if job.get("build_generation")
            else self.bindings.get(namespace)
        )
        adapter: object | None = None
        if binding is not None and not job.get("cleanup"):
            if job["stage"] == "process":
                adapter = self.processor_for(job, binding)
            elif job["stage"] == "chunk":
                adapter = binding.chunker
            elif job["stage"] == "embed":
                # Embedding is bounded by the worker pool, independently of queries.
                return ResourceGrant(lambda: None)
        return self.acquire_adapter(adapter)

    def close(self) -> None:
        self.legacy_index.close()

    def reconcile_bindings(self) -> None:
        for namespace in list(self.bindings):
            if namespace not in self.lifecycle.namespaces:
                self.bindings.pop(namespace)
        for key in list(self.build_bindings):
            if key[0] not in self.lifecycle.namespaces:
                self.build_bindings.pop(key)
        for namespace, record in self.lifecycle.namespaces.items():
            revision = record.get("active_generation")
            binding = self.build_bindings.get((namespace, revision)) if revision else None
            if binding is not None:
                self.bind_if_current(namespace, binding)
                self.index_errors.discard(namespace)
            retained = {
                revision,
                record.get("building", {}).get("generation"),
                *record.get("retiring_generations", []),
            }
            for key in list(self.build_bindings):
                if key[0] == namespace and key[1] not in retained:
                    self.build_bindings.pop(key, None)

    def binding(self, namespace: str) -> NamespaceBinding:
        self.lifecycle.require_modern_namespace(namespace)
        binding = self.bindings.get(namespace)
        if binding is None:
            raise CapabilityUnavailable(f"namespace {namespace!r} needs open_namespace binding")
        return binding

    def bind_if_current(self, namespace: str, binding: NamespaceBinding) -> None:
        """Adopt supplied adapters even when a committed configuration lost its ACK."""
        with self.lifecycle.condition:
            record = self.lifecycle.namespaces.get(namespace)
            if record is None:
                return
            try:
                binding.verify(
                    namespace, record.get("pending_manifest", record.get("manifest", {}))
                )
            except (NamespaceCompatibilityError, KeyError):
                return
            self.bindings[namespace] = binding
            self.lifecycle.condition.notify_all()

    def index(
        self, namespace: str, incarnation: str | None = None, generation: str | None = None
    ) -> ChunkIndex:
        with self.lifecycle.condition:
            if incarnation is None:
                record = self.lifecycle.namespace_record(namespace)
                incarnation = record["incarnation"]
                generation = generation or record.get("active_generation")
            name = "ns_" + str(incarnation) + ("_" + generation if generation else "")
            if name not in self.collections:
                self.collections[name] = self.legacy_index.collection(name)
            return self.collections[name]

    def dense_config(self, namespace: str) -> dict[str, Any] | None:
        return (
            self.lifecycle.namespace_record(namespace)
            .get("manifest", {})
            .get("index", {})
            .get("dense")
        )

    def processor_for(
        self, job: dict[str, Any], binding: NamespaceBinding | None
    ) -> Processor | None:
        if binding is None:
            return None
        return next(
            (p for p in binding.processors if binding.descriptions[id(p)] == job.get("processor")),
            None,
        )

    def embed_documents(self, embedder: Embedder, texts: Sequence[str]) -> list[list[float]]:
        result: list[list[float]] = []
        for start in range(0, len(texts), 128):
            batch = texts[start : start + 128]
            try:
                result.extend(
                    self.validate_vectors(
                        embedder.embed_documents(batch), len(batch), embedder.dimension
                    )
                )
            except MFSError:
                raise
            except Exception as error:
                raise EmbeddingFailed(f"document embedding failed: {error}") from error
        return result

    def chunk_text(
        self,
        chunker: Chunker,
        text: str,
        source_map: SourceMap,
        deadline: SearchDeadline | None = None,
    ) -> Sequence[ChunkRange]:
        with self.lifecycle.condition:
            while (lease := self.acquire_adapter(chunker)) is None:
                if self.lifecycle.stopping:
                    raise Closed("MFS instance is closing")
                if deadline is not None:
                    deadline.check()
                self.lifecycle.condition.wait(0.05)
        try:
            if deadline is not None:
                deadline.check()
            return chunker.chunk(text, source_map)
        finally:
            lease.release()

    def embed_query(
        self, embedder: Embedder, text: str, deadline: SearchDeadline | None = None
    ) -> list[float]:
        try:
            if deadline is not None:
                deadline.check()
            return self.validate_vectors([embedder.embed_query(text)], 1, embedder.dimension)[0]
        except MFSError:
            raise
        except Exception as error:
            raise EmbeddingFailed(f"query embedding failed: {error}") from error

    @staticmethod
    def matching_embedder(
        binding: NamespaceBinding | None, dense: dict[str, Any] | None
    ) -> Embedder:
        if dense is None or binding is None or binding.embedder is None:
            raise CapabilityUnavailable("namespace has no bound dense index")
        if (
            dense["embedding_space"] != binding.embedder.embedding_space
            or dense["dimension"] != binding.embedder.dimension
        ):
            raise NamespaceCompatibilityError("Embedder declaration changed after binding")
        return binding.embedder

    @contextmanager
    def query(
        self, namespace: str
    ) -> Generator[tuple[dict[str, Any], NamespaceBinding | None, ChunkIndex]]:
        lifecycle = self.lifecycle
        with lifecycle.condition:
            lifecycle.require_modern_namespace(namespace)
            record = copy.deepcopy(lifecycle.namespaces[namespace])
            record["_publications"] = {
                identity: (
                    snapshot,
                    lifecycle.targets[identity].get(
                        "input_version", lifecycle.targets[identity]["revision"]
                    ),
                )
                for identity, snapshot in lifecycle.visible.items()
                if identity.namespace == namespace
            }
            if "pending_manifest" in record and record["indexing"] != "off":
                raise IndexUnavailable(f"{namespace}: index configuration is being rebuilt")
            if namespace in self.index_errors and record["indexing"] != "off":
                raise IndexUnavailable(f"{namespace}: collection requires explicit reindex")
            incarnation = record["incarnation"]
            index = self.index(namespace, incarnation, record.get("active_generation"))
            binding = self.bindings.get(namespace)
            lifecycle.queries[incarnation] = lifecycle.queries.get(incarnation, 0) + 1
            generation_key = (incarnation, record.get("active_generation"))
            lifecycle.generation_queries[generation_key] = (
                lifecycle.generation_queries.get(generation_key, 0) + 1
            )
        try:
            yield record, binding, index
        finally:
            with lifecycle.condition:
                lifecycle.queries[incarnation] -= 1
                if not lifecycle.queries[incarnation]:
                    del lifecycle.queries[incarnation]
                lifecycle.generation_queries[generation_key] -= 1
                if not lifecycle.generation_queries[generation_key]:
                    del lifecycle.generation_queries[generation_key]
                lifecycle.condition.notify_all()

    @staticmethod
    def validate_vectors(
        vectors: Sequence[Sequence[float]], count: int, dimension: int
    ) -> list[list[float]]:
        if len(vectors) != count:
            raise EmbeddingFailed(f"Embedder returned {len(vectors)} vectors; expected {count}")
        result: list[list[float]] = []
        for vector in vectors:
            if len(vector) != dimension:
                raise EmbeddingFailed(
                    f"Embedder returned dimension {len(vector)}; expected {dimension}"
                )
            converted = [float(item) for item in vector]
            if not all(math.isfinite(item) for item in converted):
                raise EmbeddingFailed("Embedder returned non-finite values")
            result.append(converted)
        return result
