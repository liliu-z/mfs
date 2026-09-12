from __future__ import annotations

from typing import Any

import blake3

from ._catalog import Catalog
from ._documents import parse_source_map, source_location
from ._index import IndexRow
from ._lifecycle import Lifecycle
from ._preparation import Preparation
from ._runtime import NamespaceRuntime
from ._validation import validate_chunk_ranges
from ._vector_cache import VectorCache
from ._work import (
    Chunked,
    ChunkPlan,
    Cleaned,
    Embedded,
    ExecutionPermit,
    Published,
    Rebuilt,
    StepResult,
)
from .errors import CorruptState, SourceChanged
from .types import DocumentId


class Indexing:
    """Compute/write one index stage; only Lifecycle commits the resulting state."""

    def __init__(
        self,
        catalog: Catalog,
        lifecycle: Lifecycle,
        runtime: NamespaceRuntime,
        preparation: Preparation,
    ) -> None:
        self.catalog, self.lifecycle = catalog, lifecycle
        self.runtime, self.preparation = runtime, preparation
        self.vector_cache = VectorCache(catalog)

    def execute(self, permit: ExecutionPermit) -> StepResult | None:
        identity, job = permit.identity, permit.payload
        with self.lifecycle.condition:
            if not self.lifecycle.current(identity, job):
                return
        if job.get("cleanup"):
            index = self.runtime.index(identity.namespace, job.get("incarnation"))
            index.delete_document(identity, incarnation=job.get("incarnation"))
            index.flush()
            return Cleaned()
        kind, stage = job["kind"], job["stage"]
        if kind == "rebuild":
            self.rebuild(permit)
            return Rebuilt()
        index = (
            self.runtime.index(identity.namespace, job.get("incarnation"))
            if kind != "drop"
            else None
        )
        if stage == "drop":
            if job.get("legacy_cleanup") and self.runtime.legacy_index.client.has_collection(
                self.runtime.legacy_index.collection_name
            ):
                self.runtime.legacy_index.delete_namespace(identity.namespace)
            for incarnation in job["incarnations"]:
                old_index = self.runtime.index(identity.namespace, incarnation)
                if old_index.client.has_collection(old_index.collection_name):
                    old_index.drop()
        elif stage == "delete":
            assert index is not None
            if index.client.has_collection(index.collection_name):
                index.delete_document(identity, incarnation=job.get("incarnation"))
                index.flush()
        elif job["indexing"] == "off":
            return Published(indexed=False)
        elif stage == "chunk":
            record = self.snapshot(identity, job)
            text = self.preparation.read_text(record, permit)
            assert permit.binding is not None
            with self.runtime.chunker_lock:
                ranges = validate_chunk_ranges(
                    text,
                    permit.binding.chunker.chunk(text, parse_source_map(record)),
                )
            encoded = text.encode()
            plan = [
                ChunkPlan(
                    ordinal=i,
                    text_start=r.text_start,
                    text_end=r.text_end,
                    text_hash=blake3.blake3(encoded[r.text_start : r.text_end]).hexdigest(),
                )
                for i, r in enumerate(ranges)
            ]
            return Chunked(plan)
        elif stage == "embed":
            assert index is not None
            record = self.snapshot(identity, job)
            encoded = self.preparation.read_text(record, permit).encode()
            batch = int(job.get("completed_batches", 0))
            plan = job["plan"][batch * 128 : (batch + 1) * 128]
            texts: dict[str, str] = {}
            for chunk in plan:
                raw = encoded[chunk["text_start"] : chunk["text_end"]]
                if blake3.blake3(raw).hexdigest() != chunk["text_hash"]:
                    raise SourceChanged("text reference changed after chunk planning; sync again")
                texts[chunk["text_hash"]] = raw.decode()
            vectors: dict[str, list[float]] = {}
            assert permit.binding is not None
            dense = permit.binding.manifest["index"]["dense"]
            if dense:
                prefix = self.vector_cache.prefix(job, dense)
                with self.lifecycle.condition:
                    if not self.lifecycle.current(identity, job):
                        return
                    vectors = self.vector_cache.get(prefix, list(texts), int(dense["dimension"]))
                candidates = index.vector_candidates(tuple(texts))
                with self.lifecycle.condition:
                    for row in candidates:
                        owner = DocumentId(row["namespace"], row["doc_id"])
                        if self.lifecycle.visible_snapshot(owner, row["snapshot_id"]) or (
                            owner == identity and row["snapshot_id"] == job["snapshot_id"]
                        ):
                            vectors[row["text_hash"]] = self.runtime.validate_vectors(
                                [row["dense_vector"]], 1, int(dense["dimension"])
                            )[0]
                missing = [h for h in texts if h not in vectors]
                if missing:
                    computed = self.runtime.embed_documents(
                        self.runtime.matching_embedder(permit.binding, dense),
                        [texts[h] for h in missing],
                    )
                    vectors.update(zip(missing, computed, strict=True))
                with self.lifecycle.condition:
                    if not self.lifecycle.current(identity, job):
                        return
                    self.vector_cache.put(prefix, job["incarnation"], vectors)
            source_map = parse_source_map(record)
            rows: list[IndexRow] = []
            for chunk in plan:
                location = source_location(source_map, chunk["text_start"], chunk["text_end"])
                rows.append(
                    IndexRow(
                        namespace=identity.namespace,
                        doc_id=identity.doc_id,
                        ordinal=chunk["ordinal"],
                        text=texts[chunk["text_hash"]],
                        text_hash=chunk["text_hash"],
                        text_start=chunk["text_start"],
                        text_end=chunk["text_end"],
                        dense_vector=vectors.get(chunk["text_hash"], []),
                        snapshot_id=job["snapshot_id"],
                        source_location=dict(version=1, sources=list(location.sources)),
                        media_type=job["media_type"],
                        incarnation=job["incarnation"],
                    )
                )
            with self.lifecycle.condition:
                if not self.lifecycle.current(identity, job):
                    return
            # Complete vectors are durable in Milvus. These rows stay invisible until publish.
            index.insert(rows)
            index.flush()
            return Embedded(batch + 1, batch + 1 == job["batches"])
        elif stage == "publish":
            assert index is not None
            index.publish(identity, job["snapshot_id"], job["incarnation"], len(job["plan"]))
        else:
            raise CorruptState(f"unknown task stage {stage!r}")
        return Published(indexed=kind == "upsert")

    def snapshot(self, identity: DocumentId, job: dict[str, Any]) -> dict[str, Any]:
        record = self.catalog.get_document(identity.namespace, identity.doc_id)
        if record is None or record.get("revision") != job["revision"]:
            raise SourceChanged("processed text no longer belongs to the current source")
        return record

    def rebuild(self, permit: ExecutionPermit) -> None:
        identity, job = permit.identity, permit.payload
        for incarnation in job.get("retired_incarnations", []):
            if incarnation != job["incarnation"]:
                retired = self.runtime.index(identity.namespace, incarnation)
                if retired.client.has_collection(retired.collection_name):
                    retired.drop()
        dense = job["manifest"]["index"]["dense"]
        index = self.runtime.index(identity.namespace, job["incarnation"])
        index.recreate(dense_dimension=int(dense["dimension"]) if dense else None)
        legacy = self.runtime.legacy_index
        if job.get("legacy_cleanup") and legacy.client.has_collection(legacy.collection_name):
            legacy.delete_namespace(identity.namespace)
