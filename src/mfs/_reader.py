# pyright: reportPrivateUsage=false
from __future__ import annotations

import codecs
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import blake3

from ._filters import compile_filters
from ._index import SearchHit
from ._json import copy_json
from ._validation import validate_chunk_ranges
from .errors import IndexUnavailable, InvalidQuery, SourceUnavailable
from .types import (
    AnyOf,
    ByDocumentId,
    ByNamespace,
    Chunk,
    Consistency,
    DocumentId,
    Filter,
    GrepBudget,
    GrepItem,
    GrepResult,
    Match,
    NamespaceKind,
    SearchItem,
    SearchMode,
    SearchResult,
    Select,
    SourceLocation,
    SourceMap,
    SourceSpan,
    UnderPath,
)

if TYPE_CHECKING:
    from ._core import MFS


def _line_map(text: str) -> SourceMap:
    offset = 0
    spans: list[SourceSpan] = []
    for number, line in enumerate(text.splitlines(keepends=True), 1):
        end = offset + len(line.encode())
        spans.append(SourceSpan(offset, end, {"kind": "lines", "start": number, "end": number}))
        offset = end
    return SourceMap(1, tuple(spans))


def _read(mfs: MFS, record: dict[str, Any], maximum: int) -> tuple[str, bool, int]:
    reference = record.get("grep_ref") or record["text_ref"]
    path = mfs._artifacts.path(reference["path"]) if reference["owned"] else Path(reference["path"])
    try:
        with path.open("rb") as stream:
            raw = stream.read(maximum + 1)
        truncated = len(raw) > maximum
        decoder = codecs.getincrementaldecoder(reference.get("encoding", "utf-8"))()
        return decoder.decode(raw[:maximum], final=not truncated), truncated, min(len(raw), maximum)
    except (OSError, UnicodeError) as error:
        raise SourceUnavailable(f"cannot read search text {path}: {error}") from error


def grep(
    mfs: MFS,
    filters: Sequence[Filter],
    select: Select,
    limit: int | None,
    budget: GrepBudget,
) -> GrepResult[Any]:
    mfs._validate_query_options(select, limit, search=False)
    if any(isinstance(v, bool) or not isinstance(v, int) or v < 1 for v in asdict(budget).values()):
        raise InvalidQuery("grep budgets must be positive integers")
    with mfs._condition:
        names: dict[str, NamespaceKind] = {
            n: cast(NamespaceKind, r["kind"]) for n, r in mfs._tasks.namespaces.items()
        }
    compiled = compile_filters(filters, names, search=False)
    for namespace in selected_namespaces(filters, set(names)):
        mfs._require_modern_namespace(namespace)
    for text_filter in compiled.text:
        mfs._text_matches("", text_filter)
    items: list[GrepItem[Any]] = []
    bytes_read = matches_seen = documents_seen = 0
    truncated = False
    maximum_items = limit if limit is not None else budget.max_documents
    for namespace, doc_id, stored in mfs._catalog.iter_documents(compiled.sql, compiled.params):
        identity = DocumentId(namespace, doc_id)
        with mfs._condition:
            current = mfs._tasks.targets.get(identity, {})
            if (
                current.get("revision") != stored.get("revision")
                or namespace not in mfs._tasks.namespaces
                or mfs._excluded(namespace, doc_id)
            ):
                continue
        if documents_seen >= budget.max_documents:
            truncated = True
            break
        documents_seen += 1
        record = dict(stored)
        text = ""
        source_map = mfs._source_map(record)
        if compiled.text or select != "doc_id":
            remaining = min(budget.max_file_bytes, budget.max_bytes - bytes_read)
            if remaining <= 0:
                truncated = True
                break
            text, cut, used = _read(mfs, record, remaining)
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
            record.update(text=text, source_map=mfs._source_map_json(source_map))
        ranges: list[tuple[int, int]] = []
        failed = False
        for text_filter in compiled.text:
            found = mfs._text_matches(
                text, text_filter, limit=budget.max_matches - matches_seen + 1
            )
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
            Match(a, b, mfs._source_location(source_map, a, b)) for a, b in _merge_ranges(ranges)
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
            with mfs._chunker_lock:
                chunks = validate_chunk_ranges(
                    text, mfs._binding(namespace).chunker.chunk(text, source_map)
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
                            mfs._source_location(source_map, chunk.text_start, chunk.text_end),
                        ),
                        located,
                    )
                )
        with mfs._condition:
            if (
                mfs._tasks.targets.get(identity, {}).get("revision") != stored.get("revision")
                or namespace not in mfs._tasks.namespaces
                or mfs._excluded(namespace, doc_id)
            ):
                continue
            for value in values:
                if len(items) == maximum_items:
                    return GrepResult(tuple(items), True)
                items.append(value)
        if matches_seen >= budget.max_matches:
            truncated = True
            break
    return GrepResult(tuple(items), truncated)


def selected_namespaces(filters: Sequence[Filter], names: set[str]) -> set[str]:
    def selected(item: Filter) -> set[str]:
        if isinstance(item, ByNamespace):
            return set(item.namespaces)
        if isinstance(item, ByDocumentId):
            return {i.namespace for i in item.ids}
        if isinstance(item, UnderPath):
            return {item.namespace}
        if isinstance(item, AnyOf):
            return set[str]().union(*(selected(f) for f in item.filters))
        return names

    result = set(names)
    for item in filters:
        result &= selected(item)
    return result


def search(
    mfs: MFS,
    text: str,
    filters: Sequence[Filter],
    mode: SearchMode,
    select: Select,
    limit: int,
    consistency: Consistency,
    timeout: float | None,
) -> SearchResult[Any]:
    if not isinstance(cast(object, text), str) or not text or len(text.encode()) > 65536:
        raise InvalidQuery("search text must be 1..65536 UTF-8 bytes")
    if mode not in ("bm25", "vector", "hybrid") or consistency not in ("strong", "eventual"):
        raise InvalidQuery("invalid search mode or consistency")
    mfs._validate_query_options(select, limit, search=True)
    mfs._validate_timeout(timeout)
    if select == "doc":
        raise InvalidQuery("ranked search returns chunk/doc_id; use read for a document")
    with mfs._condition:
        names: dict[str, NamespaceKind] = {
            n: cast(NamespaceKind, r["kind"]) for n, r in mfs._tasks.namespaces.items()
        }
        chosen = sorted(selected_namespaces(filters, set(names)))
    expressions = compile_filters(filters, names, search=True).expressions
    for namespace in chosen:
        mfs._require_modern_namespace(namespace)
    if consistency == "strong":
        mfs._wait_ready(timeout, set(chosen))
    routes: list[list[SearchHit]] = []
    more = False
    for namespace in chosen:
        if mfs._tasks.namespaces[namespace]["indexing"] == "off":
            continue
        if "pending_manifest" in mfs._tasks.namespaces[namespace]:
            raise IndexUnavailable(f"{namespace}: index configuration is being rebuilt")
        if namespace in mfs._index_errors:
            raise IndexUnavailable(f"{namespace}: collection requires explicit reindex")
        vector = mfs._embed_query(namespace, text) if mode in ("vector", "hybrid") else None
        index = mfs._namespace_index(namespace)
        candidate_limit = min(1000, max(100, limit * 10))
        while True:
            channels: list[list[SearchHit]] = []
            extra = False
            if mode in ("bm25", "hybrid"):
                hits, available = index.search(
                    text, mode="bm25", expressions=expressions, limit=candidate_limit
                )
                channels.append(hits)
                extra |= available
            if vector is not None:
                hits, available = index.search(
                    vector, mode="vector", expressions=expressions, limit=candidate_limit
                )
                channels.append(hits)
                extra |= available
            with mfs._condition:
                channels = [
                    [
                        h
                        for h in channel
                        if mfs._tasks.visible.get(DocumentId(h["namespace"], h["doc_id"]))
                        == h["snapshot_id"]
                        and h["namespace"] in mfs._tasks.namespaces
                        and not mfs._excluded(h["namespace"], h["doc_id"])
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
        routes.append(ranked)
        more |= extra
    ranked = _rrf(routes) if len(routes) > 1 else routes[0] if routes else []
    items: list[SearchItem[Any]] = []
    seen: set[DocumentId] = set()
    with mfs._condition:
        for hit in ranked:
            identity = DocumentId(hit["namespace"], hit["doc_id"])
            if mfs._tasks.visible.get(identity) != hit["snapshot_id"]:
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
    return SearchResult(tuple(items[:limit]), more or len(items) > limit)


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
