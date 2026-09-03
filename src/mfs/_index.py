# pyright: reportMissingTypeStubs=false, reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
from __future__ import annotations

import json
import math
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, TypedDict

from pymilvus import DataType, Function, FunctionType, MilvusClient

from .errors import IndexFailed
from .types import DocumentId

COLLECTION = "chunks"
OUTPUT_FIELDS = ["namespace", "doc_id", "ordinal", "text", "text_start", "text_end"]


class IndexRow(TypedDict):
    namespace: str
    doc_id: str
    ordinal: int
    text: str
    text_start: int
    text_end: int
    dense_vector: list[float]


class SearchHit(TypedDict):
    namespace: str
    doc_id: str
    ordinal: int
    text: str
    text_start: int
    text_end: int
    score: float


def dense_config(embedding_space: str, dimension: int) -> dict[str, object]:
    return {
        "embedding_space": embedding_space,
        "dimension": dimension,
        "metric": "COSINE",
    }


def index_config(chunker: dict[str, object], dense: dict[str, object] | None) -> dict[str, object]:
    return {
        "version": 1,
        "chunker": chunker,
        "bm25": {
            "analyzer": {"tokenizer": "standard", "filter": ["lowercase"]},
            "k1": 1.2,
            "b": 0.75,
        },
        "dense": dense,
    }


class ChunkIndex:
    def __init__(self, path: Path) -> None:
        try:
            self.client: Any = MilvusClient(uri=str(path))
        except Exception as error:
            raise IndexFailed(f"failed to open Milvus Lite: {error}") from error

    def close(self) -> None:
        try:
            self.client.close()
        except Exception as error:
            raise IndexFailed(f"failed to close Milvus Lite: {error}") from error

    def has_valid_collection(self, *, dense_dimension: int | None) -> bool:
        try:
            if not self.client.has_collection(COLLECTION):
                return False
            description = self.client.describe_collection(COLLECTION)
            if description.get("auto_id") is not True or description.get("enable_dynamic_field"):
                return False
            fields = description.get("fields", [])
            by_name = {field.get("name"): field for field in fields}
            expected_types = {
                "id": DataType.INT64,
                "namespace": DataType.VARCHAR,
                "doc_id": DataType.VARCHAR,
                "ordinal": DataType.INT64,
                "text": DataType.VARCHAR,
                "text_start": DataType.INT64,
                "text_end": DataType.INT64,
                "sparse_vector": DataType.SPARSE_FLOAT_VECTOR,
            }
            if dense_dimension is not None:
                expected_types["dense_vector"] = DataType.FLOAT_VECTOR
            if set(by_name) != set(expected_types):
                return False
            if any(
                by_name[name].get("type") != data_type for name, data_type in expected_types.items()
            ):
                return False
            if not by_name["id"].get("is_primary") or not by_name["id"].get("auto_id"):
                return False
            if int(by_name["namespace"].get("params", {}).get("max_length", -1)) != 255:
                return False
            if int(by_name["doc_id"].get("params", {}).get("max_length", -1)) != 2048:
                return False
            text_params = by_name["text"].get("params", {})
            if (
                int(text_params.get("max_length", -1)) != 65535
                or str(text_params.get("enable_analyzer", "")).lower() != "true"
            ):
                return False
            if dense_dimension is not None:
                params = by_name["dense_vector"].get("params", {})
                if int(params.get("dim", -1)) != dense_dimension:
                    return False
            functions = description.get("functions", [])
            if len(functions) != 1:
                return False
            function = functions[0]
            if (
                function.get("type") != FunctionType.BM25
                or function.get("input_field_names") != ["text"]
                or function.get("output_field_names") != ["sparse_vector"]
            ):
                return False
            expected_indexes = {
                "sparse_vector": ("SPARSE_INVERTED_INDEX", "BM25"),
                "namespace": ("INVERTED", "NONE"),
                "doc_id": ("INVERTED", "NONE"),
            }
            if dense_dimension is not None:
                expected_indexes["dense_vector"] = ("AUTOINDEX", "COSINE")
            if set(self.client.list_indexes(COLLECTION)) != set(expected_indexes):
                return False
            for index_name, expected in expected_indexes.items():
                actual = self.client.describe_index(COLLECTION, index_name)
                if (actual.get("index_type"), actual.get("metric_type")) != expected:
                    return False
            return True
        except Exception:
            return False

    def recreate(self, *, dense_dimension: int | None) -> None:
        try:
            if self.client.has_collection(COLLECTION):
                self.client.drop_collection(COLLECTION)
            schema = MilvusClient.create_schema(auto_id=True, enable_dynamic_field=False)
            schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
            schema.add_field(field_name="namespace", datatype=DataType.VARCHAR, max_length=255)
            schema.add_field(field_name="doc_id", datatype=DataType.VARCHAR, max_length=2048)
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
                collection_name=COLLECTION, schema=schema, index_params=indexes
            )
            self.client.load_collection(COLLECTION)
        except Exception as error:
            raise IndexFailed(f"failed to create chunk collection: {error}") from error

    def replace(self, document_id: DocumentId, rows: Sequence[IndexRow]) -> None:
        self.delete_document(document_id)
        if rows:
            self.insert(rows)
        self.flush()
        actual = self.count_document(document_id)
        if actual != len(rows):
            raise IndexFailed(
                f"chunk row validation failed for {document_id}: expected {len(rows)}, got {actual}"
            )

    def insert(self, rows: Sequence[IndexRow]) -> None:
        if not rows:
            return
        data: list[dict[str, object]] = []
        for row in rows:
            item: dict[str, object] = {key: row[key] for key in OUTPUT_FIELDS}
            if row.get("dense_vector"):
                item["dense_vector"] = row["dense_vector"]
            data.append(item)
        try:
            for start in range(0, len(data), 1000):
                self.client.insert(COLLECTION, data[start : start + 1000])
        except Exception as error:
            raise IndexFailed(f"failed to insert chunks: {error}") from error

    def delete_document(self, document_id: DocumentId) -> None:
        expression = (
            f"namespace == {_literal(document_id.namespace)} and "
            f"doc_id == {_literal(document_id.doc_id)}"
        )
        try:
            self.client.delete(COLLECTION, filter=expression)
        except Exception as error:
            raise IndexFailed(f"failed to delete document chunks: {error}") from error

    def delete_namespace(self, namespace: str) -> None:
        try:
            self.client.delete(COLLECTION, filter=f"namespace == {_literal(namespace)}")
            self.flush()
        except Exception as error:
            raise IndexFailed(f"failed to delete namespace chunks: {error}") from error

    def flush(self) -> None:
        try:
            self.client.flush(COLLECTION)
        except Exception as error:
            raise IndexFailed(f"failed to flush chunks: {error}") from error

    def count_document(self, document_id: DocumentId) -> int:
        expression = (
            f"namespace == {_literal(document_id.namespace)} and "
            f"doc_id == {_literal(document_id.doc_id)}"
        )
        try:
            rows = self.client.query(COLLECTION, filter=expression, output_fields=["count(*)"])
            return int(rows[0]["count(*)"]) if rows else 0
        except Exception as error:
            raise IndexFailed(f"failed to count document chunks: {error}") from error

    def scan(self) -> list[dict[str, object]]:
        result: list[dict[str, object]] = []
        try:
            iterator = self.client.query_iterator(
                collection_name=COLLECTION,
                batch_size=1000,
                filter="id >= 0",
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
        documents: Sequence[DocumentId] | None,
        limit: int,
    ) -> tuple[list[SearchHit], bool]:
        batches: Iterable[Sequence[DocumentId] | None]
        if documents is None:
            batches = (None,)
        elif not documents:
            return [], False
        else:
            batches = (documents[start : start + 200] for start in range(0, len(documents), 200))
        all_hits: dict[tuple[str, str, int], SearchHit] = {}
        possibly_more = False
        for batch in batches:
            expression = _documents_expression(batch) if batch is not None else ""
            try:
                raw = self.client.search(
                    collection_name=COLLECTION,
                    data=[text_or_vector],
                    anns_field="sparse_vector" if mode == "bm25" else "dense_vector",
                    filter=expression,
                    limit=limit,
                    output_fields=OUTPUT_FIELDS,
                    search_params={"metric_type": "BM25" if mode == "bm25" else "COSINE"},
                )
            except Exception as error:
                raise IndexFailed(f"{mode} search failed: {error}") from error
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
                    score=(
                        -float(hit.get("distance", hit.get("score", 0.0)))
                        if mode == "bm25"
                        else float(hit.get("distance", hit.get("score", 0.0)))
                    ),
                )
                if not math.isfinite(parsed["score"]):
                    raise IndexFailed("search returned a non-finite score")
                key = (parsed["namespace"], parsed["doc_id"], parsed["ordinal"])
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
