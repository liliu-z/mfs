from __future__ import annotations

from typing import Any

from ._json import JSONValue, canonical_json, copy_json
from ._regex import regex_ranges
from .errors import CorruptState, InvalidFilter, InvalidPattern, InvalidQuery
from .types import SourceLocation, SourceMap, SourceSpan, TextMatch


def source_map_json(source_map: SourceMap) -> dict[str, JSONValue]:
    return {
        "version": 1,
        "spans": [
            {
                "text_start": span.text_start,
                "text_end": span.text_end,
                "source": copy_json(span.source),
            }
            for span in source_map.spans
        ],
    }


def parse_source_map(record: dict[str, Any]) -> SourceMap:
    try:
        raw = record["source_map"]
        if raw["version"] != 1:
            raise ValueError("unsupported source map version")
        spans = tuple(
            SourceSpan(
                int(item["text_start"]),
                int(item["text_end"]),
                copy_json(item["source"]),
            )
            for item in raw["spans"]
        )
        return SourceMap(1, spans)
    except Exception as error:
        raise CorruptState(f"invalid persisted source map: {error}") from error


def source_location(source_map: SourceMap, start: int, end: int) -> SourceLocation:
    sources: list[JSONValue] = []
    seen: set[bytes] = set()
    for span in source_map.spans:
        if span.text_start < end and start < span.text_end:
            encoded = canonical_json(span.source)
            if encoded not in seen:
                sources.append(copy_json(span.source))
                seen.add(encoded)
    return SourceLocation(1, tuple(sources))


def text_matches(
    text: str, text_filter: TextMatch, *, limit: int | None = None
) -> list[tuple[int, int]]:
    pattern = text_filter.pattern
    if not isinstance(identity_type(pattern), str) or not pattern or len(pattern.encode()) > 16384:
        raise InvalidFilter("TextMatch pattern must be 1..16384 UTF-8 bytes")
    try:
        sensitive = text_filter.case_sensitive or (
            text_filter.smart_case and any(c.isupper() for c in pattern)
        )
        ranges = regex_ranges(
            text,
            pattern,
            regex=text_filter.regex,
            case_sensitive=sensitive,
            limit=limit,
            whole_word=text_filter.whole_word,
        )
    except Exception as error:
        raise InvalidPattern(f"invalid RE2 pattern: {error}") from error
    offsets = [0]
    for character in text:
        offsets.append(offsets[-1] + len(character.encode()))
    return [(offsets[start], offsets[end]) for start, end in ranges]


def validate_query_options(select: str, limit: int | None, *, search: bool) -> None:
    if select not in ("doc_id", "chunk", "doc"):
        raise InvalidQuery(f"invalid select {select!r}")
    if limit is not None and (
        isinstance(identity_type(limit), bool) or not isinstance(identity_type(limit), int)
    ):
        raise InvalidQuery("limit must be an integer")
    if search and (limit is None or not 1 <= limit <= 1000):
        raise InvalidQuery("search limit must be 1..1000")
    if not search and limit is not None and not 1 <= limit <= 100000:
        raise InvalidQuery("grep limit must be 1..100000 or None")


def identity_type(value: object) -> object:
    return value
