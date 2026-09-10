from __future__ import annotations

from .types import DocumentId


class MFSError(Exception):
    code = "MFSError"

    def __init__(
        self,
        message: str,
        *,
        document_id: DocumentId | None = None,
        path: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.document_id = document_id
        self.path = path

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


class InvalidNamespace(MFSError):
    code = "InvalidNamespace"


class NamespaceNotFound(MFSError):
    code = "NamespaceNotFound"


class NamespaceConflict(MFSError):
    code = "NamespaceConflict"


class WrongNamespaceKind(MFSError):
    code = "WrongNamespaceKind"


class InvalidDocumentId(MFSError):
    code = "InvalidDocumentId"


class InvalidPath(MFSError):
    code = "InvalidPath"


class RootOverlap(MFSError):
    code = "RootOverlap"


class SourceUnavailable(MFSError):
    code = "SourceUnavailable"


class SourceChanged(MFSError):
    code = "SourceChanged"


class UnsupportedMediaType(MFSError):
    code = "UnsupportedMediaType"


class ProcessingFailed(MFSError):
    code = "ProcessingFailed"


class EmbeddingFailed(MFSError):
    code = "EmbeddingFailed"


class StorageFailed(MFSError):
    code = "StorageFailed"


class IndexFailed(MFSError):
    code = "IndexFailed"


class InvalidFilter(MFSError):
    code = "InvalidFilter"


class InvalidPattern(MFSError):
    code = "InvalidPattern"


class InvalidQuery(MFSError):
    code = "InvalidQuery"


class IndexUnavailable(MFSError):
    code = "IndexUnavailable"


class CapabilityUnavailable(MFSError):
    code = "CapabilityUnavailable"


class InvalidConfiguration(MFSError):
    code = "InvalidConfiguration"


class InstanceLocked(MFSError):
    code = "InstanceLocked"


class SchemaVersionUnsupported(MFSError):
    code = "SchemaVersionUnsupported"


class CorruptState(MFSError):
    code = "CorruptState"


class Closed(MFSError):
    code = "Closed"


class WaitTimeout(MFSError):
    code = "WaitTimeout"


class IdempotencyConflict(MFSError):
    code = "IdempotencyConflict"


class RetryableError(MFSError):
    """An injected adapter may raise this to request automatic bounded retries."""

    code = "RetryableError"


class OperationFailed(MFSError):
    code = "OperationFailed"

    def __init__(
        self,
        message: str,
        *,
        revision: str | None = None,
        state: str = "failed",
        error_code: str | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.revision = revision
        self.state = state
        self.error_code = error_code
        self.retryable = retryable


class Superseded(OperationFailed):
    code = "Superseded"
