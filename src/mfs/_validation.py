from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path

from ._json import JSONValue, canonical_json, copy_json
from .errors import (
    InvalidConfiguration,
    InvalidDocumentId,
    InvalidNamespace,
    InvalidPath,
    ProcessingFailed,
)
from .types import Chunker, ChunkRange, Embedder, ProcessedDocument, Processor, SourceMap

_MEDIA_TYPE = re.compile(r"^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$")


def _runtime(value: object) -> object:
    return value


def validate_namespace(namespace: str) -> str:
    value = _runtime(namespace)
    if not isinstance(value, str) or not value or "\0" in value:
        raise InvalidNamespace("namespace must be a non-empty string without NUL")
    if len(value.encode("utf-8")) > 255:
        raise InvalidNamespace("namespace exceeds 255 UTF-8 bytes")
    return value


def validate_internal_id(doc_id: str) -> str:
    value = _runtime(doc_id)
    if not isinstance(value, str) or not value or "\0" in value:
        raise InvalidDocumentId("doc_id must be a non-empty string without NUL")
    if len(value.encode("utf-8")) > 2048:
        raise InvalidDocumentId("doc_id exceeds 2048 UTF-8 bytes")
    return value


def validate_external_path(path: str, *, allow_root: bool) -> str:
    value = _runtime(path)
    if not isinstance(value, str) or not value or "\0" in value or "\\" in value:
        raise InvalidPath("path must be a non-empty POSIX path without NUL or backslash")
    if value == ".":
        if allow_root:
            return value
        raise InvalidPath("'.' is only valid as a sync root")
    if value.startswith("/"):
        raise InvalidPath("absolute paths are not allowed", path=value)
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise InvalidPath("path contains an empty, '.' or '..' segment", path=value)
    if len(value.encode("utf-8")) > 2048:
        raise InvalidPath("path exceeds 2048 UTF-8 bytes", path=value)
    return value


def normalized_media_type(value: str) -> str:
    result = value.partition(";")[0].strip().lower()
    if not _MEDIA_TYPE.fullmatch(result):
        raise ProcessingFailed(f"invalid media type: {value!r}")
    return result


def processor_description(processor: Processor) -> dict[str, JSONValue]:
    return {
        "id": processor.id,
        "version": processor.version,
        "options": copy_json(processor.options),
    }


def chunker_description(chunker: Chunker) -> dict[str, JSONValue]:
    return {
        "id": chunker.id,
        "version": chunker.version,
        "options": copy_json(chunker.options),
    }


def validate_processors(processors: Sequence[Processor]) -> tuple[Processor, ...]:
    media_types: set[str] = set()
    suffixes: set[str] = set()
    result: list[Processor] = []
    for processor in processors:
        try:
            if (
                not isinstance(_runtime(processor.id), str)
                or not processor.id
                or not isinstance(_runtime(processor.version), str)
                or not processor.version
            ):
                raise ValueError("id and version must be non-empty")
            canonical_json(processor.options)
            if not processor.media_types:
                raise ValueError("media_types must not be empty")
            own_media = set(processor.media_types)
            for media_type in processor.media_types:
                if media_type != media_type.lower() or not _MEDIA_TYPE.fullmatch(media_type):
                    raise ValueError(f"invalid media type: {media_type!r}")
                if media_type in media_types:
                    raise ValueError(f"duplicate media type: {media_type}")
                media_types.add(media_type)
            for suffix, media_type in processor.suffix_media_types.items():
                if (
                    not suffix.startswith(".")
                    or suffix != suffix.lower()
                    or suffix.count(".") != 1
                    or "/" in suffix
                ):
                    raise ValueError(f"invalid suffix: {suffix!r}")
                if media_type not in own_media:
                    raise ValueError(f"suffix media type is not owned: {media_type}")
                if suffix in suffixes:
                    raise ValueError(f"duplicate suffix: {suffix}")
                suffixes.add(suffix)
        except (AttributeError, TypeError, ValueError) as error:
            raise InvalidConfiguration(f"invalid Processor: {error}") from error
        result.append(processor)
    return tuple(result)


def validate_chunker(chunker: Chunker) -> Chunker:
    try:
        if (
            not isinstance(_runtime(chunker.id), str)
            or not chunker.id
            or not isinstance(_runtime(chunker.version), str)
            or not chunker.version
        ):
            raise ValueError("id and version must be non-empty")
        canonical_json(chunker.options)
    except (AttributeError, TypeError, ValueError) as error:
        raise InvalidConfiguration(f"invalid Chunker: {error}") from error
    return chunker


def validate_embedder(embedder: Embedder | None) -> Embedder | None:
    if embedder is None:
        return None
    if (
        not isinstance(_runtime(embedder.embedding_space), str)
        or not embedder.embedding_space
        or isinstance(embedder.dimension, bool)
        or not isinstance(_runtime(embedder.dimension), int)
        or embedder.dimension <= 0
    ):
        raise InvalidConfiguration("Embedder space must be non-empty and dimension positive")
    return embedder


def validate_processed(value: ProcessedDocument) -> ProcessedDocument:
    try:
        encoded = value.text.encode("utf-8", errors="strict")
    except (AttributeError, UnicodeEncodeError) as error:
        raise ProcessingFailed("Processor returned text that is not valid UTF-8") from error
    validate_source_map(value.source_map, len(encoded))
    if value.text_path is not None:
        try:
            if value.text_path.read_text(encoding="utf-8-sig") != value.text:
                raise ProcessingFailed("text_path must contain the returned text")
        except (OSError, UnicodeError) as error:
            raise ProcessingFailed(f"text_path is not readable UTF-8: {error}") from error
    return value


def validate_source_map(source_map: SourceMap, text_bytes: int) -> None:
    if source_map.version != 1:
        raise ProcessingFailed("source map version must be 1")
    previous_end = 0
    for span in source_map.spans:
        if not (0 <= span.text_start <= span.text_end <= text_bytes):
            raise ProcessingFailed("source map span is outside document text")
        if span.text_start < previous_end:
            raise ProcessingFailed("source map spans must be monotonic and non-overlapping")
        try:
            canonical_json(span.source)
        except ValueError as error:
            raise ProcessingFailed(f"source map contains invalid JSON: {error}") from error
        previous_end = span.text_end


def validate_chunk_ranges(text: str, ranges: Sequence[ChunkRange]) -> tuple[ChunkRange, ...]:
    text_bytes = len(text.encode("utf-8"))
    result = tuple(ranges)
    if text_bytes == 0:
        if result:
            raise ProcessingFailed("Chunker must return no ranges for empty text")
        return result
    if not result or result[0].text_start != 0 or result[-1].text_end != text_bytes:
        raise ProcessingFailed("Chunk ranges must cover the complete text")
    boundaries = _utf8_boundaries(text)
    previous_start = -1
    previous_end = 0
    for chunk_range in result:
        start, end = chunk_range.text_start, chunk_range.text_end
        if start <= previous_start or start > previous_end or not (0 <= start < end <= text_bytes):
            raise ProcessingFailed("Chunk ranges are invalid, unordered, or leave a gap")
        if start not in boundaries or end not in boundaries:
            raise ProcessingFailed("Chunk range splits a UTF-8 code point")
        if end - start > 65535:
            raise ProcessingFailed("Chunk text exceeds 65535 UTF-8 bytes")
        previous_start = start
        previous_end = end
    return result


def _utf8_boundaries(text: str) -> set[int]:
    result = {0}
    offset = 0
    for character in text:
        offset += len(character.encode("utf-8"))
        result.add(offset)
    return result


def suffix_for(path: str | Path) -> str:
    return Path(path).suffix.lower()


def validate_suffix_map(mapping: Mapping[str, str]) -> None:
    del mapping
