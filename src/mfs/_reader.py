from __future__ import annotations

import codecs
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

import blake3

from ._artifacts import ArtifactStore
from ._catalog import Catalog
from ._documents import (
    parse_source_map,
    source_location,
    source_map_json,
    text_matches,
    validate_query_options,
)
from ._filters import compile_filters
from ._index import SearchHit
from ._json import copy_json
from ._lifecycle import ReadView
from ._runtime import NamespaceRuntime
from ._search_execution import SearchDeadline
from ._validation import validate_chunk_ranges
from .errors import CorruptState, InvalidQuery, SourceUnavailable
from .types import (
    Chunk,
    Consistency,
    Document,
    DocumentId,
    Filter,
    GrepBudget,
    GrepItem,
    GrepResult,
    Match,
    SearchItem,
    SearchMode,
    SearchResult,
    Select,
    SourceLocation,
    SourceMap,
    SourceSpan,
)


def _line_map(text: str) -> SourceMap:
    offset = 0
    spans: list[SourceSpan] = []
    for number, line in enumerate(text.splitlines(keepends=True), 1):
        end = offset + len(line.encode())
        spans.append(SourceSpan(offset, end, {"kind": "lines", "start": number, "end": number}))
        offset = end
    return SourceMap(1, tuple(spans))


def _merge_ranges(ranges: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    if not ranges:
        return []
    result: list[tuple[int, int]] = []
    for start, end in sorted(set(ranges)):
        if result and start <= result[-1][1]:
            previous_start, previous_end = result[-1]
            result[-1] = (previous_start, max(previous_end, end))
        else:
            result.append((start, end))
    return result


def _rrf(channels: Sequence[Sequence[SearchHit]]) -> list[SearchHit]:
    combined: dict[tuple[str, str, int], SearchHit] = {}
    scores: dict[tuple[str, str, int], float] = {}
    for channel in channels:
        ordered = sorted(
            channel,
            key=lambda hit: (
                -hit["score"],
                hit["namespace"].encode(),
                hit["doc_id"].encode(),
                hit["ordinal"],
            ),
        )
        for rank, hit in enumerate(ordered, start=1):
            key = (hit["namespace"], hit["doc_id"], hit["ordinal"])
            combined[key] = hit
            scores[key] = scores.get(key, 0.0) + 1.0 / (60 + rank)
    result: list[SearchHit] = []
    for key, hit in combined.items():
        item = SearchHit(**hit)
        item["score"] = scores[key]
        result.append(item)
    return sorted(
        result,
        key=lambda hit: (
            -hit["score"],
            hit["namespace"].encode(),
            hit["doc_id"].encode(),
            hit["ordinal"],
        ),
    )


class Reader:
    """Query immutable metadata views and verify eligibility before returning results."""

    def __init__(
        self, catalog: Catalog, artifacts: ArtifactStore, runtime: NamespaceRuntime, view: ReadView
    ) -> None:
        self.catalog, self.artifacts, self.runtime, self.view = catalog, artifacts, runtime, view

    def read(self, document_id: DocumentId) -> Document | None:
        self.view.require_modern_namespace(document_id.namespace)
        record = self.catalog.get_document(document_id.namespace, document_id.doc_id)
        if record is None or not self.view.current_text(document_id, record.get("revision")):
            return None
        document = self._document(document_id, record)
        return document if self.view.current_text(document_id, record.get("revision")) else None

    def _document(self, document_id: DocumentId, record: dict[str, Any]) -> Document:
        value = record["source"].get("object")
        object_path = self.artifacts.path(value) if value is not None else None
        try:
            original = object_path.read_bytes() if object_path is not None else None
            text = self.artifacts.read_text(record, grep=True)
            reference = record.get("grep_ref") or record.get("text_ref")
            source_map = parse_source_map(record)
            if (
                reference
                and not reference["owned"]
                and (blake3.blake3(text.encode()).hexdigest() != record.get("text_hash"))
            ):
                source_map = _line_map(text)
            return Document(
                id=document_id,
                snapshot_id=str(record["snapshot_id"]),
                media_type=str(record["media_type"]),
                text=text,
                source_map=source_map,
                original=original,
            )
        except OSError as error:
            raise CorruptState(f"internal object is unavailable: {error}") from error

    def _read(self, record: dict[str, Any], maximum: int) -> tuple[str, bool, int]:
        reference = record.get("grep_ref") or record["text_ref"]
        path = (
            self.artifacts.path(reference["path"])
            if reference["owned"]
            else Path(reference["path"])
        )
        try:
            with path.open("rb") as stream:
                raw = stream.read(maximum + 1)
            truncated = len(raw) > maximum
            decoder = codecs.getincrementaldecoder(reference.get("encoding", "utf-8"))()
            return (
                decoder.decode(raw[:maximum], final=not truncated),
                truncated,
                min(len(raw), maximum),
            )
        except (OSError, UnicodeError) as error:
            raise SourceUnavailable(f"cannot read search text {path}: {error}") from error

    def grep(
        self,
        namespace: str,
        filters: Sequence[Filter],
        select: Select,
        limit: int | None,
        budget: GrepBudget,
    ) -> GrepResult[Any]:
        validate_query_options(select, limit, search=False)
        if any(
            isinstance(v, bool) or not isinstance(v, int) or v < 1 for v in asdict(budget).values()
        ):
            raise InvalidQuery("grep budgets must be positive integers")
        record = self.view.namespace(namespace)
        compiled = compile_filters(filters, namespace, record["kind"], search=False)
        for text_filter in compiled.text:
            text_matches("", text_filter)
        items: list[GrepItem[Any]] = []
        bytes_read = matches_seen = documents_seen = 0
        truncated = False
        maximum_items = limit if limit is not None else budget.max_documents
        for namespace, doc_id, stored in self.catalog.iter_documents(compiled.sql, compiled.params):
            identity = DocumentId(namespace, doc_id)
            with self.view.condition:
                if not self.view.current_text(identity, stored.get("revision")):
                    continue
            if documents_seen >= budget.max_documents:
                truncated = True
                break
            documents_seen += 1
            record = dict(stored)
            text = ""
            source_map = parse_source_map(record)
            if compiled.text or select != "doc_id":
                remaining = min(budget.max_file_bytes, budget.max_bytes - bytes_read)
                if remaining <= 0:
                    truncated = True
                    break
                text, cut, used = self._read(record, remaining)
                bytes_read += used
                truncated |= cut
                reference = record.get("grep_ref") or record["text_ref"]
                if (not reference["owned"] or record.get("grep_ref")) and (
                    blake3.blake3(text.encode()).hexdigest() != record.get("text_hash")
                ):
                    source_map = _line_map(text)
                if cut:
                    end = len(text.encode())
                    source_map = SourceMap(
                        1,
                        tuple(
                            SourceSpan(span.text_start, min(span.text_end, end), span.source)
                            for span in source_map.spans
                            if span.text_start < end
                        ),
                    )
                record.update(text=text, source_map=source_map_json(source_map))
            ranges: list[tuple[int, int]] = []
            failed = False
            for text_filter in compiled.text:
                found = text_matches(text, text_filter, limit=budget.max_matches - matches_seen + 1)
                if not found:
                    failed = True
                    break
                allowed = budget.max_matches - matches_seen
                truncated |= len(found) > allowed
                ranges.extend(found[:allowed])
                matches_seen += min(allowed, len(found))
            if failed:
                continue
            matches = tuple(
                Match(a, b, source_location(source_map, a, b)) for a, b in _merge_ranges(ranges)
            )
            if select == "doc_id":
                values: list[GrepItem[Any]] = [GrepItem(identity, matches)]
            elif select == "doc":
                # Preserve the bounded text already read, rather than re-reading the whole file.
                from .types import Document

                values = [
                    GrepItem(
                        Document(
                            identity,
                            record["snapshot_id"],
                            record["media_type"],
                            text,
                            source_map,
                            None,
                        ),
                        matches,
                    )
                ]
            else:
                with self.runtime.chunker_lock:
                    chunks = validate_chunk_ranges(
                        text, self.runtime.binding(namespace).chunker.chunk(text, source_map)
                    )
                encoded = text.encode()
                values = []
                for ordinal, chunk in enumerate(chunks):
                    located = tuple(
                        m
                        for m in matches
                        if m.text_start < chunk.text_end and chunk.text_start < m.text_end
                    )
                    if matches and not located:
                        continue
                    values.append(
                        GrepItem(
                            Chunk(
                                identity,
                                record["snapshot_id"],
                                ordinal,
                                encoded[chunk.text_start : chunk.text_end].decode(),
                                chunk.text_start,
                                chunk.text_end,
                                source_location(source_map, chunk.text_start, chunk.text_end),
                            ),
                            located,
                        )
                    )
            with self.view.condition:
                if not self.view.current_text(identity, stored.get("revision")):
                    continue
                for value in values:
                    if len(items) == maximum_items:
                        return GrepResult(tuple(items), True)
                    items.append(value)
            if matches_seen >= budget.max_matches:
                truncated = True
                break
        return GrepResult(tuple(items), truncated)

    def search(
        self,
        namespace: str,
        text: str,
        filters: Sequence[Filter],
        mode: SearchMode,
        select: Select,
        limit: int,
        consistency: Consistency,
        deadline: SearchDeadline,
    ) -> SearchResult[Any]:
        deadline.check()
        if not isinstance(cast(object, text), str) or not text or len(text.encode()) > 65536:
            raise InvalidQuery("search text must be 1..65536 UTF-8 bytes")
        if mode not in ("bm25", "vector", "hybrid") or consistency not in ("strong", "eventual"):
            raise InvalidQuery("invalid search mode or consistency")
        validate_query_options(select, limit, search=True)
        if select == "doc":
            raise InvalidQuery("ranked search returns chunk/doc_id; use read for a document")
        initial_kind = self.view.namespace(namespace)["kind"]
        expressions = compile_filters(filters, namespace, initial_kind, search=True).expressions
        deadline.check()
        if consistency == "strong":
            self.view.wait_ready(deadline.remaining(), {namespace})
        deadline.check()
        with self.runtime.query(namespace) as (record, binding, index):
            if record["kind"] != initial_kind:
                expressions = compile_filters(
                    filters, namespace, record["kind"], search=True
                ).expressions
            if record["indexing"] == "off":
                return SearchResult((), False)
            vector = (
                self.runtime.embed_query(
                    self.runtime.matching_embedder(binding, record["manifest"]["index"]["dense"]),
                    text,
                )
                if mode in ("vector", "hybrid")
                else None
            )
            candidate_limit = min(1000, max(100, limit * 10))
            while True:
                deadline.check()
                channels: list[list[SearchHit]] = []
                extra = False
                if mode in ("bm25", "hybrid"):
                    hits, available = index.search(
                        text,
                        mode="bm25",
                        expressions=expressions,
                        limit=candidate_limit,
                        deadline=deadline,
                    )
                    channels.append(hits)
                    extra |= available
                    deadline.check()
                if vector is not None:
                    hits, available = index.search(
                        vector,
                        mode="vector",
                        expressions=expressions,
                        limit=candidate_limit,
                        deadline=deadline,
                    )
                    channels.append(hits)
                    extra |= available
                    deadline.check()
                with self.view.condition:
                    channels = [
                        [
                            h
                            for h in channel
                            if self.view.visible(
                                DocumentId(h["namespace"], h["doc_id"]), h["snapshot_id"]
                            )
                        ]
                        for channel in channels
                    ]
                ranked = _rrf(channels) if mode == "hybrid" else channels[0]
                count = (
                    len(ranked)
                    if select == "chunk"
                    else len({(h["namespace"], h["doc_id"]) for h in ranked})
                )
                if count >= limit or not extra or candidate_limit == 1000:
                    break
                candidate_limit = min(1000, candidate_limit * 2)
            deadline.check()
            items: list[SearchItem[Any]] = []
            seen: set[DocumentId] = set()
            with self.view.condition:
                for hit in ranked:
                    deadline.check()
                    identity = DocumentId(hit["namespace"], hit["doc_id"])
                    if not self.view.visible(identity, hit["snapshot_id"]):
                        continue
                    if select == "doc_id":
                        if identity in seen:
                            continue
                        seen.add(identity)
                        value: Any = identity
                    else:
                        value = Chunk(
                            identity,
                            hit["snapshot_id"],
                            hit["ordinal"],
                            hit["text"],
                            hit["text_start"],
                            hit["text_end"],
                            SourceLocation(
                                1, tuple(copy_json(v) for v in hit["source_location"]["sources"])
                            ),
                        )
                    items.append(SearchItem(value, hit["score"], ()))
            return SearchResult(tuple(items[:limit]), extra or len(items) > limit)
