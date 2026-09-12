from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypedDict

from ._namespace import NamespaceBinding
from .processing import Cancellation
from .types import DocumentId


@dataclass(frozen=True)
class FileWork:
    document_id: DocumentId
    revision: str


@dataclass(frozen=True)
class NamespaceWork:
    namespace: str
    revision: str
    action: Literal["drop", "rebuild"]


@dataclass(frozen=True)
class ExecutionPermit:
    subject: FileWork | NamespaceWork
    incarnation: str | None
    token: str
    cancellation: Cancellation
    payload: dict[str, Any]
    binding: NamespaceBinding | None

    @property
    def identity(self) -> DocumentId:
        if isinstance(self.subject, FileWork):
            return self.subject.document_id
        return DocumentId(self.subject.namespace, "")


class ChunkPlan(TypedDict):
    ordinal: int
    text_start: int
    text_end: int
    text_hash: str


@dataclass(frozen=True)
class Prepared:
    record: dict[str, Any]


@dataclass(frozen=True)
class Chunked:
    plan: list[ChunkPlan]


@dataclass(frozen=True)
class Embedded:
    completed_batches: int
    final: bool


@dataclass(frozen=True)
class Published:
    indexed: bool


@dataclass(frozen=True)
class Cleaned:
    pass


@dataclass(frozen=True)
class Rebuilt:
    pass


type StepResult = Prepared | Chunked | Embedded | Published | Cleaned | Rebuilt


@dataclass(frozen=True)
class SourceInput:
    path: Path
    content_hash: str
    size: int
    mtime_ns: int | None
    media_type: str
    processor: dict[str, Any]
