from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .processing import ProcessingContext

from ._json import JSONValue

type NamespaceKind = Literal["internal", "external"]
type MutationOutcome = Literal["added", "updated", "unchanged", "removed", "not_found"]
type IndexState = Literal["ready", "pending", "dirty", "mismatch"]
type Consistency = Literal["strong", "eventual"]
type TaskStage = Literal["process", "chunk", "embed", "publish", "delete", "drop", "rebuild"]
type TaskState = Literal[
    "pending", "running", "retry_wait", "failed", "blocked", "cancelled", "succeeded"
]
type SnapshotId = str
type SyncSkipReason = Literal[
    "excluded", "too_large", "symlink", "special_file", "unsupported_media_type"
]
type Select = Literal["doc_id", "chunk", "doc"]
type SearchMode = Literal["bm25", "vector", "hybrid"]
type IndexingMode = Literal["off", "bm25", "hybrid"]
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
class NamespaceConfiguration:
    namespace: str
    kind: NamespaceKind
    root: Path | None
    indexing: IndexingMode
    paused: bool
    manifest: JSONValue
    pending_manifest: JSONValue
    max_file_bytes: int | None


@dataclass(frozen=True, slots=True)
class MutationReport:
    id: DocumentId
    outcome: MutationOutcome
    index_ready: bool
    revision: str | None = None


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
    ready: bool = False
    pending_count: int = 0
    failed_count: int = 0


@dataclass(frozen=True, slots=True)
class DocumentStatus:
    id: DocumentId
    revision: str
    text_revision: str | None
    indexed_revision: str | None
    stage: TaskStage
    state: TaskState
    attempts: int
    error: str | None
    next_retry_at: float | None
    completed_batches: int = 0
    total_batches: int = 0
    executing: bool = False
    content_hash: str | None = None
    media_type: str | None = None
    source_size: int | None = None
    source_mtime_ns: int | None = None
    error_detail: TaskError | None = None
    progress: Progress | None = None
    artifacts: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReindexReport:
    documents: int
    chunks: int
    dense_enabled: bool


@dataclass(frozen=True, slots=True)
class ProcessedDocument:
    text: str
    source_map: SourceMap
    artifacts: Mapping[str, Path] = field(default_factory=lambda: dict[str, Path]())
    text_path: Path | None = None
    grep_path: Path | None = None


@dataclass(frozen=True, slots=True)
class ChunkRange:
    text_start: int
    text_end: int


@dataclass(frozen=True, slots=True)
class SyncPolicy:
    exclude_globs: tuple[str, ...] = ()
    max_file_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class IgnoreRule:
    rule_id: str
    pattern: str
    action: Literal["include", "exclude"] = "exclude"


@dataclass(frozen=True, slots=True)
class RuleSet:
    revision: str
    rules: tuple[IgnoreRule, ...]


@dataclass(frozen=True, slots=True)
class GrepBudget:
    max_documents: int = 10000
    max_bytes: int = 64 * 1024 * 1024
    max_file_bytes: int = 8 * 1024 * 1024
    max_matches: int = 10000


@dataclass(frozen=True, slots=True)
class TaskError:
    code: str
    message: str
    retryable: bool


@dataclass(frozen=True, slots=True)
class Progress:
    completed: float
    total: float | None = None
    unit: str | None = None


@dataclass(frozen=True, slots=True)
class GCPolicy:
    enabled: bool = True
    interval: float = 3600.0
    idle_seconds: float = 30.0
    grace_seconds: float = 3600.0
    batch_files: int = 32
    batch_seconds: float = 0.05
    cycle_files: int = 256
    cycle_seconds: float = 1.0


@dataclass(frozen=True, slots=True)
class GCReport:
    deleted: int = 0
    skipped: int = 0
    busy: bool = False
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ScopeStatus:
    total: int
    states: Mapping[str, int]
    stages: Mapping[str, int]


class Filter:
    __slots__ = ()


def _tuple_or_one[T](value: T | Sequence[T]) -> tuple[T, ...]:
    if isinstance(value, (str, bytes)):
        return (value,)  # type: ignore[return-value]
    if isinstance(value, Sequence):
        return tuple(value)  # pyright: ignore[reportUnknownArgumentType]
    return (value,)


@dataclass(frozen=True, slots=True, init=False)
class AnyOf(Filter):
    filters: tuple[Filter, ...]

    def __init__(self, filters: Sequence[Filter]) -> None:
        object.__setattr__(self, "filters", tuple(filters))


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
    smart_case: bool = False
    whole_word: bool = False


@dataclass(frozen=True, slots=True)
class PathPrefix(Filter):
    value: str


@dataclass(frozen=True, slots=True)
class PathSuffix(Filter):
    value: str


@dataclass(frozen=True, slots=True)
class NamePrefix(Filter):
    value: str


@dataclass(frozen=True, slots=True)
class NameSuffix(Filter):
    value: str


@dataclass(frozen=True, slots=True, init=False)
class ByExtension(Filter):
    extensions: tuple[str, ...]

    def __init__(self, extensions: str | Sequence[str]) -> None:
        object.__setattr__(self, "extensions", _tuple_or_one(extensions))


@dataclass(frozen=True, slots=True, init=False)
class ByMediaType(Filter):
    media_types: tuple[str, ...]

    def __init__(self, media_types: str | Sequence[str]) -> None:
        object.__setattr__(self, "media_types", _tuple_or_one(media_types))


@dataclass(frozen=True, slots=True)
class GrepItem[T]:
    value: T
    matches: tuple[Match, ...]


@dataclass(frozen=True, slots=True)
class SearchItem[T]:
    value: T
    score: float
    matches: tuple[Match, ...]


@dataclass(frozen=True, slots=True)
class GrepResult[T]:
    items: tuple[GrepItem[T], ...]
    truncated: bool


@dataclass(frozen=True, slots=True)
class SearchResult[T]:
    items: tuple[SearchItem[T], ...]
    truncated: bool


@runtime_checkable
class LegacyProcessor(Protocol):
    id: str
    version: str
    options: JSONValue
    media_types: tuple[str, ...]
    suffix_media_types: Mapping[str, str]

    def sniff(self, head: bytes) -> str | None: ...

    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument: ...


@runtime_checkable
class ContextProcessor(Protocol):
    id: str
    version: str
    options: JSONValue
    media_types: tuple[str, ...]
    suffix_media_types: Mapping[str, str]

    def sniff(self, head: bytes) -> str | None: ...

    def process(
        self, staged_path: Path, media_type: str, context: ProcessingContext
    ) -> ProcessedDocument: ...


type Processor = LegacyProcessor | ContextProcessor


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
