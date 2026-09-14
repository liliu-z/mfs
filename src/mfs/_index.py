# pyright: reportMissingTypeStubs=false, reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
from __future__ import annotations

import json
import math
import threading
from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, NotRequired, TypedDict
from weakref import WeakValueDictionary

import blake3
from milvus_lite.server_manager import server_manager_instance
from pymilvus import DataType, Function, FunctionType, MilvusClient

from ._backend_deadlines import Deadline
from ._search_execution import SearchDeadline
from .errors import IndexFailed
from .types import DocumentId

COLLECTION = "chunks"
OUTPUT_FIELDS = [
    "namespace",
    "doc_id",
    "ordinal",
    "text",
    "text_hash",
    "text_start",
    "text_end",
    "snapshot_id",
    "source_location",
    "media_type",
    "incarnation",
    "source_path",
    "source_name",
    "source_ext",
    "source_path_rev",
    "source_name_rev",
]


class IndexRow(TypedDict):
    namespace: str
    doc_id: str
    ordinal: int
    text: str
    text_start: int
    text_end: int
    dense_vector: list[float]
    snapshot_id: NotRequired[str]
    source_location: NotRequired[dict[str, Any]]
    media_type: NotRequired[str]
    incarnation: NotRequired[str]
    text_hash: NotRequired[str]


class SearchHit(TypedDict):
    namespace: str
    doc_id: str
    ordinal: int
    text: str
    text_start: int
    text_end: int
    score: float
    snapshot_id: str
    source_location: dict[str, Any]


def dense_config(embedding_space: str, dimension: int) -> dict[str, object]:
    return {
        "embedding_space": embedding_space,
        "dimension": dimension,
        "metric": "COSINE",
    }


def index_config(chunker: dict[str, object], dense: dict[str, object] | None) -> dict[str, object]:
    return {
        "version": 2,
        "chunker": chunker,
        "bm25": {
            "analyzer": {"tokenizer": "standard", "filter": ["lowercase"]},
            "k1": 1.2,
            "b": 0.75,
        },
        "dense": dense,
    }


class _SerializedClient:
    """Serialize calls per collection while independent collections overlap.

    Milvus Lite 3.2.1 adapter/grpc/server.py requires single-writer use per
    collection. Protect reads too, including lazy collection opening. Connection
    closure drains all calls across collection handles.
    """

    def __init__(self, client: Any) -> None:
        self._client = client
        self._lock = threading.RLock()
        self._owner = self
        self._condition = threading.Condition()
        self._locks: WeakValueDictionary[str, Any] = WeakValueDictionary()
        self._locks[COLLECTION] = self._lock
        self._local = threading.local()
        self._active = 0
        self._closed = False

    def collection(self, name: str) -> _SerializedClient:
        handle = object.__new__(_SerializedClient)
        handle._client = self._client
        handle._owner = self._owner
        with self._owner._condition:
            lock = self._owner._locks.get(name)
            if lock is None:
                lock = threading.RLock()
                self._owner._locks[name] = lock
            handle._lock = lock
        return handle

    @contextmanager
    def deadline(self, deadline: Deadline) -> Generator[None]:
        local = self._owner._local
        previous = getattr(local, "deadline", None)
        local.deadline = deadline
        try:
            yield
        finally:
            local.deadline = previous

    def close(self) -> None:
        owner = self._owner
        with owner._condition:
            owner._closed = True
            owner._condition.wait_for(lambda: owner._active == 0)
        owner._client.close()

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._client, name)
        if not callable(attribute):
            return attribute

        def invoke(*args: Any, **kwargs: Any) -> Any:
            deadline: Deadline | None = kwargs.pop("_deadline", None) or getattr(
                self._owner._local, "deadline", None
            )
            owner = self._owner
            with owner._condition:
                if owner._closed:
                    raise IndexFailed("backend client is closed")
                owner._active += 1
            try:
                while not self._lock.acquire(timeout=0.05):
                    if deadline is not None:
                        deadline.check()
                try:
                    if deadline is not None:
                        deadline.check()
                        # Lite's in-process gRPC handlers ignore cancellation. A wire
                        # timeout would release this lock while the server still writes.
                        # The caller/watchdog has its own deadline; retain this actual
                        # call until the handler returns, then reject late results.
                        kwargs["timeout"] = None
                    result = attribute(*args, **kwargs)
                    if deadline is not None:
                        deadline.check()
                    return result
                finally:
                    self._lock.release()
            finally:
                with owner._condition:
                    owner._active -= 1
                    owner._condition.notify_all()

        return invoke


class ChunkIndex:
    def __init__(self, path: Path) -> None:
        self._path = str(path)
        self.collection_name = COLLECTION
        self._owns_client = True
        try:
            self.client: Any = _SerializedClient(MilvusClient(uri=str(path)))
        except Exception as error:
            raise IndexFailed(f"failed to open Milvus Lite: {error}") from error

    def collection(self, name: str) -> ChunkIndex:
        handle = object.__new__(ChunkIndex)
        handle._path = self._path
        handle.client = self.client.collection(name)
        handle.collection_name = name
        handle._owns_client = False
        return handle

    def drop(self) -> None:
        try:
            self.client.drop_collection(self.collection_name)
        except Exception as error:
            raise IndexFailed(f"failed to drop collection: {error}") from error

    def close(self) -> None:
        if not self._owns_client:
            return
        try:
            self.client.close()
        except Exception as error:
            raise IndexFailed(f"failed to close Milvus Lite: {error}") from error
        finally:
            # MilvusClient.close only releases its connection, not the embedded server/database.
            server_manager_instance.release_server(self._path)

    def load(self) -> None:
        try:
            self.client.load_collection(self.collection_name)
        except Exception as error:
            raise IndexFailed(f"failed to load chunk collection: {error}") from error

    def has_valid_collection(self, *, dense_dimension: int | None) -> bool:
        try:
            if not self.client.has_collection(self.collection_name):
                return False
            description = self.client.describe_collection(self.collection_name)
            fields = {f["name"]: f for f in description["fields"]}
            expected = set(OUTPUT_FIELDS) | {"id", "sparse_vector"}
            if dense_dimension is not None:
                expected.add("dense_vector")
            if (
                set(fields) != expected
                or description.get("auto_id")
                or description.get("enable_dynamic_field")
            ):
                return False
            if fields["id"]["type"] != DataType.VARCHAR or not fields["id"].get("is_primary"):
                return False
            if (
                dense_dimension is not None
                and int(fields["dense_vector"].get("params", {}).get("dim", -1)) != dense_dimension
            ):
                return False
            expected_types = {name: DataType.VARCHAR for name in expected}
            expected_types.update(
                ordinal=DataType.INT64,
                text_start=DataType.INT64,
                text_end=DataType.INT64,
                sparse_vector=DataType.SPARSE_FLOAT_VECTOR,
                source_location=DataType.JSON,
            )
            if dense_dimension is not None:
                expected_types["dense_vector"] = DataType.FLOAT_VECTOR
            if any(fields[name]["type"] != kind for name, kind in expected_types.items()):
                return False
            text_params = fields["text"].get("params", {})
            if (
                int(text_params.get("max_length", -1)) != 65535
                or str(text_params.get("enable_analyzer", "")).lower() != "true"
            ):
                return False
            functions = description.get("functions", [])
            if len(functions) != 1 or (
                functions[0].get("type") != FunctionType.BM25
                or functions[0].get("input_field_names") != ["text"]
                or functions[0].get("output_field_names") != ["sparse_vector"]
            ):
                return False
            expected_indexes = {
                "sparse_vector": ("SPARSE_INVERTED_INDEX", "BM25"),
                "namespace": ("INVERTED", "NONE"),
                "doc_id": ("INVERTED", "NONE"),
            }
            if dense_dimension is not None:
                expected_indexes["dense_vector"] = ("AUTOINDEX", "COSINE")
            if set(self.client.list_indexes(self.collection_name)) != set(expected_indexes):
                return False
            for name, expected_index in expected_indexes.items():
                actual = self.client.describe_index(self.collection_name, name)
                if (actual.get("index_type"), actual.get("metric_type")) != expected_index:
                    return False
            return True
        except Exception:
            return False

    def recreate(self, *, dense_dimension: int | None) -> None:
        try:
            if self.client.has_collection(self.collection_name):
                self.client.drop_collection(self.collection_name)
            schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field(
                field_name="id", datatype=DataType.VARCHAR, max_length=64, is_primary=True
            )
            schema.add_field(field_name="namespace", datatype=DataType.VARCHAR, max_length=255)
            schema.add_field(field_name="doc_id", datatype=DataType.VARCHAR, max_length=2048)
            for name, length in (
                ("snapshot_id", 64),
                ("text_hash", 64),
                ("incarnation", 64),
                ("media_type", 255),
                ("source_path", 2048),
                ("source_name", 2048),
                ("source_ext", 2048),
                ("source_path_rev", 2048),
                ("source_name_rev", 2048),
            ):
                schema.add_field(field_name=name, datatype=DataType.VARCHAR, max_length=length)
            schema.add_field(field_name="source_location", datatype=DataType.JSON)
            schema.add_field(field_name="ordinal", datatype=DataType.INT64)
            schema.add_field(
                field_name="text",
                datatype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                analyzer_params={"tokenizer": "standard", "filter": ["lowercase"]},
            )
            schema.add_field(field_name="text_start", datatype=DataType.INT64)
            schema.add_field(field_name="text_end", datatype=DataType.INT64)
            schema.add_field(field_name="sparse_vector", datatype=DataType.SPARSE_FLOAT_VECTOR)
            if dense_dimension is not None:
                schema.add_field(
                    field_name="dense_vector",
                    datatype=DataType.FLOAT_VECTOR,
                    dim=dense_dimension,
                )
            schema.add_function(
                Function(
                    name="text_bm25",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=["sparse_vector"],
                )
            )
            indexes = MilvusClient.prepare_index_params()
            indexes.add_index(
                field_name="sparse_vector",
                index_name="sparse_bm25",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="BM25",
                params={"inverted_index_algo": "DAAT_MAXSCORE", "bm25_k1": 1.2, "bm25_b": 0.75},
            )
            if dense_dimension is not None:
                indexes.add_index(
                    field_name="dense_vector",
                    index_name="dense_cosine",
                    index_type="AUTOINDEX",
                    metric_type="COSINE",
                )
            indexes.add_index(
                field_name="namespace", index_name="namespace_scalar", index_type="INVERTED"
            )
            indexes.add_index(
                field_name="doc_id", index_name="doc_id_scalar", index_type="INVERTED"
            )
            self.client.create_collection(
                collection_name=self.collection_name, schema=schema, index_params=indexes
            )
            self.client.load_collection(self.collection_name)
        except Exception as error:
            raise IndexFailed(f"failed to create chunk collection: {error}") from error

    def replace(self, document_id: DocumentId, rows: Sequence[IndexRow]) -> None:
        # Only this method's complete payload enters Milvus; retry uses identical primary keys.
        self.insert(rows)
        if rows:
            keep = str(rows[0].get("snapshot_id", ""))
            incarnation = str(rows[0].get("incarnation", ""))
            expression = (
                f"({_documents_expression([document_id])}) and "
                f"(snapshot_id != {_literal(keep)} or incarnation != {_literal(incarnation)} "
                f"or ordinal >= {len(rows)})"
            )
            try:
                self.client.delete(self.collection_name, filter=expression)
            except Exception as error:
                raise IndexFailed(f"failed to retire previous chunks: {error}") from error
        else:
            self.delete_document(document_id)
        self.flush()
        actual = self.count_document(document_id)
        if actual != len(rows):
            raise IndexFailed(
                f"chunk row validation failed for {document_id}: expected {len(rows)}, got {actual}"
            )

    def insert(self, rows: Sequence[IndexRow]) -> None:
        data: list[dict[str, object]] = []
        for row in rows:
            doc = row["doc_id"]
            name = doc.rsplit("/", 1)[-1]
            snapshot = row.get("snapshot_id", "")
            incarnation = row.get("incarnation", "")
            key = json.dumps(
                [row["namespace"], incarnation, doc, snapshot, row["ordinal"]], ensure_ascii=False
            )
            item: dict[str, object] = {
                "id": blake3.blake3(key.encode()).hexdigest(),
                "namespace": row["namespace"],
                "doc_id": doc,
                "ordinal": row["ordinal"],
                "text": row["text"],
                "text_hash": row.get("text_hash", blake3.blake3(row["text"].encode()).hexdigest()),
                "text_start": row["text_start"],
                "text_end": row["text_end"],
                "snapshot_id": snapshot,
                "incarnation": incarnation,
                "media_type": row.get("media_type", "text/plain"),
                "source_location": row.get("source_location", {"version": 1, "sources": []}),
                "source_path": doc,
                "source_name": name,
                "source_ext": Path(doc).suffix.lower(),
                "source_path_rev": doc[::-1],
                "source_name_rev": name[::-1],
            }
            if row.get("dense_vector"):
                item["dense_vector"] = row["dense_vector"]
            data.append(item)
        try:
            for start in range(0, len(data), 1000):
                self.client.upsert(self.collection_name, data[start : start + 1000])
        except Exception as error:
            raise IndexFailed(f"failed to upsert chunks: {error}") from error

    def delete_document(self, document_id: DocumentId, *, incarnation: str | None = None) -> None:
        expression = (
            f"namespace == {_literal(document_id.namespace)} and "
            f"doc_id == {_literal(document_id.doc_id)}"
        )
        if incarnation is not None:
            expression += f" and incarnation == {_literal(incarnation)}"
        try:
            self.client.delete(self.collection_name, filter=expression)
        except Exception as error:
            raise IndexFailed(f"failed to delete document chunks: {error}") from error

    def vector_candidates(self, hashes: Sequence[str]) -> list[dict[str, Any]]:
        if not hashes:
            return []
        try:
            return self.client.query(
                self.collection_name,
                filter="text_hash in " + json.dumps(list(hashes)),
                output_fields=["namespace", "doc_id", "snapshot_id", "text_hash", "dense_vector"],
                limit=16384,
            )
        except Exception as error:
            raise IndexFailed(f"failed to read reusable vectors: {error}") from error

    def publish(self, document_id: DocumentId, snapshot: str, incarnation: str, count: int) -> None:
        scope = (
            f"({_documents_expression([document_id])}) and "
            f"snapshot_id == {_literal(snapshot)} and incarnation == {_literal(incarnation)}"
        )
        try:
            self.client.delete(self.collection_name, filter=f"({scope}) and ordinal >= {count}")
        except Exception as error:
            raise IndexFailed(f"failed to retire previous chunks: {error}") from error
        self.flush()
        rows = self.client.query(self.collection_name, filter=scope, output_fields=["count(*)"])
        if int(rows[0]["count(*)"]) != count:
            raise IndexFailed("publication is missing chunk rows")

    def delete_snapshot(self, document_id: DocumentId, snapshot: str, incarnation: str) -> None:
        expression = (
            f"({_documents_expression([document_id])}) and "
            f"snapshot_id == {_literal(snapshot)} and incarnation == {_literal(incarnation)}"
        )
        try:
            self.client.delete(self.collection_name, filter=expression)
            self.flush()
        except Exception as error:
            raise IndexFailed(f"failed to retire snapshot chunks: {error}") from error

    def delete_namespace(self, namespace: str, *, incarnation: str | None = None) -> None:
        try:
            expression = f"namespace == {_literal(namespace)}"
            if incarnation is not None:
                expression += f" and incarnation == {_literal(incarnation)}"
            self.client.delete(self.collection_name, filter=expression)
            self.flush()
        except Exception as error:
            raise IndexFailed(f"failed to delete namespace chunks: {error}") from error

    def flush(self) -> None:
        try:
            self.client.flush(self.collection_name)
        except Exception as error:
            raise IndexFailed(f"failed to flush chunks: {error}") from error

    def count_document(self, document_id: DocumentId) -> int:
        expression = (
            f"namespace == {_literal(document_id.namespace)} and "
            f"doc_id == {_literal(document_id.doc_id)}"
        )
        try:
            rows = self.client.query(
                self.collection_name, filter=expression, output_fields=["count(*)"]
            )
            return int(rows[0]["count(*)"]) if rows else 0
        except Exception as error:
            raise IndexFailed(f"failed to count document chunks: {error}") from error

    def scan(self) -> list[dict[str, object]]:
        result: list[dict[str, object]] = []
        try:
            iterator = self.client.query_iterator(
                collection_name=self.collection_name,
                batch_size=1000,
                filter='id != ""',
                output_fields=OUTPUT_FIELDS,
            )
            try:
                while True:
                    batch = iterator.next()
                    if not batch:
                        break
                    result.extend(batch)
            finally:
                iterator.close()
        except Exception as error:
            raise IndexFailed(f"failed to scan chunks: {error}") from error
        return result

    def search(
        self,
        text_or_vector: str | Sequence[float],
        *,
        mode: Literal["bm25", "vector"],
        documents: Sequence[DocumentId] | None = None,
        limit: int,
        expressions: Sequence[str] | None = None,
        deadline: SearchDeadline | None = None,
    ) -> tuple[list[SearchHit], bool]:
        batches: Iterable[Sequence[DocumentId] | None]
        if documents is None:
            batches = (None,)
        elif not documents:
            return [], False
        else:
            batches = (documents[start : start + 200] for start in range(0, len(documents), 200))
        all_hits: dict[tuple[str, str, str, int], SearchHit] = {}
        possibly_more = False
        compiled = (
            expressions
            if expressions is not None
            else tuple(
                _documents_expression(batch) if batch is not None else "" for batch in batches
            )
        )
        for expression in compiled:
            remaining = None if deadline is None else deadline.remaining()
            try:
                raw = self.client.search(
                    collection_name=self.collection_name,
                    data=[text_or_vector],
                    anns_field="sparse_vector" if mode == "bm25" else "dense_vector",
                    filter=expression,
                    limit=limit,
                    output_fields=OUTPUT_FIELDS,
                    search_params={"metric_type": "BM25" if mode == "bm25" else "COSINE"},
                    consistency_level="Strong",
                    timeout=remaining,
                    _deadline=deadline,
                )
            except Exception as error:
                if deadline is not None:
                    deadline.check()
                raise IndexFailed(f"{mode} search failed: {error}") from error
            if deadline is not None:
                deadline.check()
            hits = raw[0] if raw else []
            if len(hits) == limit:
                possibly_more = True
            for hit in hits:
                entity = hit.get("entity", {})
                parsed = SearchHit(
                    namespace=str(entity["namespace"]),
                    doc_id=str(entity["doc_id"]),
                    ordinal=int(entity["ordinal"]),
                    text=str(entity["text"]),
                    text_start=int(entity["text_start"]),
                    text_end=int(entity["text_end"]),
                    snapshot_id=str(entity["snapshot_id"]),
                    source_location=entity["source_location"],
                    score=(
                        -float(hit.get("distance", hit.get("score", 0.0)))
                        if mode == "bm25"
                        else float(hit.get("distance", hit.get("score", 0.0)))
                    ),
                )
                if not math.isfinite(parsed["score"]):
                    raise IndexFailed("search returned a non-finite score")
                key = (
                    parsed["namespace"],
                    parsed["doc_id"],
                    parsed["snapshot_id"],
                    parsed["ordinal"],
                )
                previous = all_hits.get(key)
                if previous is None or parsed["score"] > previous["score"]:
                    all_hits[key] = parsed
        ranked = sorted(
            all_hits.values(),
            key=lambda hit: (
                -hit["score"],
                hit["namespace"].encode(),
                hit["doc_id"].encode(),
                hit["ordinal"],
            ),
        )
        return ranked[:limit], possibly_more or len(ranked) > limit


def _literal(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _documents_expression(documents: Sequence[DocumentId]) -> str:
    return " or ".join(
        f"(namespace == {_literal(item.namespace)} and doc_id == {_literal(item.doc_id)})"
        for item in documents
    )
