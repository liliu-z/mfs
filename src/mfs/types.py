from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable

from ._json import JSONValue

type NamespaceKind = Literal["internal", "external"]
type MutationOutcome = Literal["added", "updated", "unchanged", "removed", "not_found"]
type IndexState = Literal["ready", "dirty", "mismatch"]
type SnapshotId = str
type SyncSkipReason = Literal[
    "excluded", "too_large", "symlink", "special_file", "unsupported_media_type"
]
type Select = Literal["doc_id", "chunk", "doc"]
type SearchMode = Literal["bm25", "vector", "hybrid"]
type Vector = Sequence[float]


@dataclass(frozen=True, slots=True, order=True)
class DocumentId:
    namespace: str
    doc_id: str


@dataclass(frozen=True, slots=True)
class SourceSpan:
    text_start: int
    text_end: int
    source: JSONValue


@dataclass(frozen=True, slots=True)
class SourceMap:
    version: Literal[1]
    spans: tuple[SourceSpan, ...]


@dataclass(frozen=True, slots=True)
class SourceLocation:
    version: Literal[1]
    sources: tuple[JSONValue, ...]


@dataclass(frozen=True, slots=True)
class Document:
    id: DocumentId
    snapshot_id: SnapshotId
    media_type: str
    text: str
    source_map: SourceMap
    original: bytes | None


@dataclass(frozen=True, slots=True)
class Chunk:
    document_id: DocumentId
    snapshot_id: SnapshotId
    ordinal: int
    text: str
    text_start: int
    text_end: int
    source_location: SourceLocation


@dataclass(frozen=True, slots=True)
class Match:
    text_start: int
    text_end: int
    source_location: SourceLocation


@dataclass(frozen=True, slots=True)
class NamespaceInfo:
    namespace: str
    kind: NamespaceKind
    root: Path | None = None


@dataclass(frozen=True, slots=True)
class MutationReport:
    id: DocumentId
    outcome: MutationOutcome
    index_ready: bool


@dataclass(frozen=True, slots=True)
class DropReport:
    namespace: str
    dropped: bool
    index_ready: bool


@dataclass(frozen=True, slots=True)
class SyncFailure:
    path: str
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class SyncSkipped:
    path: str
    reason: SyncSkipReason


@dataclass(frozen=True, slots=True)
class SyncReport:
    namespace: str
    path: str
    complete: bool
    changed: tuple[DocumentId, ...]
    removed: tuple[DocumentId, ...]
    failed: tuple[SyncFailure, ...]
    skipped: tuple[SyncSkipped, ...]
    index_ready: bool


@dataclass(frozen=True, slots=True)
class Status:
    namespace_count: int
    document_count: int
    index_state: IndexState
    dense_enabled: bool
    dense_available: bool


@dataclass(frozen=True, slots=True)
class ReindexReport:
    documents: int
    chunks: int
    dense_enabled: bool


@dataclass(frozen=True, slots=True)
class ProcessedDocument:
    text: str
    source_map: SourceMap


@dataclass(frozen=True, slots=True)
class ChunkRange:
    text_start: int
    text_end: int


@dataclass(frozen=True, slots=True)
class SyncPolicy:
    exclude_globs: tuple[str, ...] = ()
    max_file_bytes: int | None = None


class Filter:
    __slots__ = ()


def _tuple_or_one[T](value: T | Sequence[T]) -> tuple[T, ...]:
    if isinstance(value, (str, bytes)):
        return (value,)  # type: ignore[return-value]
    if isinstance(value, Sequence):
        return tuple(value)  # pyright: ignore[reportUnknownArgumentType]
    return (value,)


@dataclass(frozen=True, slots=True, init=False)
class ByNamespace(Filter):
    namespaces: tuple[str, ...]

    def __init__(self, namespaces: str | Sequence[str]) -> None:
        object.__setattr__(self, "namespaces", _tuple_or_one(namespaces))


@dataclass(frozen=True, slots=True, init=False)
class ByDocumentId(Filter):
    ids: tuple[DocumentId, ...]

    def __init__(self, ids: DocumentId | Sequence[DocumentId]) -> None:
        object.__setattr__(self, "ids", _tuple_or_one(ids))


@dataclass(frozen=True, slots=True)
class UnderPath(Filter):
    namespace: str
    path: str = "."


@dataclass(frozen=True, slots=True)
class TextMatch(Filter):
    pattern: str
    regex: bool = False
    case_sensitive: bool = False


@dataclass(frozen=True, slots=True)
class QueryItem[T]:
    value: T
    matches: tuple[Match, ...]


@dataclass(frozen=True, slots=True)
class SearchItem[T]:
    value: T
    score: float
    matches: tuple[Match, ...]


@dataclass(frozen=True, slots=True)
class QueryResult[T]:
    items: tuple[QueryItem[T], ...]
    truncated: bool


@dataclass(frozen=True, slots=True)
class SearchResult[T]:
    items: tuple[SearchItem[T], ...]
    truncated: bool


@runtime_checkable
class Processor(Protocol):
    id: str
    version: str
    options: JSONValue
    media_types: tuple[str, ...]
    suffix_media_types: Mapping[str, str]

    def sniff(self, head: bytes) -> str | None: ...

    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument: ...


@runtime_checkable
class Chunker(Protocol):
    id: str
    version: str
    options: JSONValue

    def chunk(self, text: str, source_map: SourceMap) -> Sequence[ChunkRange]: ...


@runtime_checkable
class Embedder(Protocol):
    embedding_space: str
    dimension: int

    def embed_documents(self, texts: Sequence[str]) -> Sequence[Vector]: ...

    def embed_query(self, text: str) -> Vector: ...
