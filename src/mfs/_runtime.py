from __future__ import annotations

import math
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ._index import ChunkIndex
from ._lifecycle import Lifecycle
from ._namespace import NamespaceBinding
from .errors import CapabilityUnavailable, EmbeddingFailed, MFSError, NamespaceCompatibilityError
from .types import Embedder, Processor


class NamespaceRuntime:
    """Own namespace adapter bindings and collection routing, including model validation."""

    def __init__(self, path: Path, lifecycle: Lifecycle) -> None:
        self.lifecycle = lifecycle
        self.bindings: dict[str, NamespaceBinding] = {}
        self.collections: dict[str, ChunkIndex] = {}
        self.index_errors: set[str] = set()
        self.chunker_lock = threading.Lock()
        self.legacy_index = ChunkIndex(path / "milvus.db")

    def close(self) -> None:
        self.legacy_index.close()

    def binding(self, namespace: str) -> NamespaceBinding:
        self.lifecycle.require_modern_namespace(namespace)
        binding = self.bindings.get(namespace)
        if binding is None:
            raise CapabilityUnavailable(f"namespace {namespace!r} needs open_namespace binding")
        return binding

    def index(self, namespace: str, incarnation: str | None = None) -> ChunkIndex:
        name = "ns_" + (incarnation or self.lifecycle.namespace_record(namespace)["incarnation"])
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

    def processor_for(self, job: dict[str, Any]) -> Processor | None:
        binding = self.bindings.get(job.get("identity", {}).get("namespace", ""))
        if binding is None:
            return None
        return next(
            (p for p in binding.processors if binding.descriptions[id(p)] == job.get("processor")),
            None,
        )

    def embed_documents(self, namespace: str, texts: Sequence[str]) -> list[list[float]]:
        embedder = self.matching_embedder(namespace)
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

    def embed_query(self, namespace: str, text: str) -> list[float]:
        embedder = self.matching_embedder(namespace)
        try:
            return self.validate_vectors([embedder.embed_query(text)], 1, embedder.dimension)[0]
        except MFSError:
            raise
        except Exception as error:
            raise EmbeddingFailed(f"query embedding failed: {error}") from error

    def matching_embedder(self, namespace: str) -> Embedder:
        binding = self.binding(namespace)
        dense = self.dense_config(namespace)
        if dense is None or binding.embedder is None:
            raise CapabilityUnavailable(f"namespace {namespace!r} has no dense index")
        if (
            dense["embedding_space"] != binding.embedder.embedding_space
            or dense["dimension"] != binding.embedder.dimension
        ):
            raise NamespaceCompatibilityError("Embedder declaration changed after binding")
        return binding.embedder

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
