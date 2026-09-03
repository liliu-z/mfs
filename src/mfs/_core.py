from __future__ import annotations

import contextlib
import math
import os
import shutil
import stat
import sys
import threading
import uuid
from collections.abc import Generator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import blake3
from filelock import FileLock, Timeout

from ._catalog import Catalog
from ._index import ChunkIndex, IndexRow, SearchHit, dense_config, index_config
from ._json import JSONValue, canonical_json, compact_json, copy_json, load_json
from ._locks import Lifecycle, ReadWriteLock
from ._regex import regex_ranges
from ._validation import (
    chunker_description,
    normalized_media_type,
    processor_description,
    suffix_for,
    validate_chunk_ranges,
    validate_chunker,
    validate_embedder,
    validate_external_path,
    validate_internal_id,
    validate_namespace,
    validate_processed,
    validate_processors,
    validate_source_map,
)
from .adapters import DefaultChunker
from .errors import (
    CapabilityUnavailable,
    CorruptState,
    EmbeddingFailed,
    IndexFailed,
    IndexUnavailable,
    InstanceLocked,
    InvalidConfiguration,
    InvalidFilter,
    InvalidPath,
    InvalidPattern,
    InvalidQuery,
    MFSError,
    NamespaceConflict,
    NamespaceNotFound,
    ProcessingFailed,
    RootOverlap,
    SchemaVersionUnsupported,
    SourceChanged,
    SourceUnavailable,
    StorageFailed,
    UnsupportedMediaType,
    WrongNamespaceKind,
)
from .types import (
    ByDocumentId,
    ByNamespace,
    Chunk,
    Chunker,
    Document,
    DocumentId,
    DropReport,
    Embedder,
    Filter,
    IndexState,
    Match,
    MutationReport,
    NamespaceInfo,
    NamespaceKind,
    Processor,
    QueryItem,
    QueryResult,
    ReindexReport,
    SearchItem,
    SearchMode,
    SearchResult,
    Select,
    SourceLocation,
    SourceMap,
    SourceSpan,
    Status,
    SyncFailure,
    SyncPolicy,
    SyncReport,
    SyncSkipped,
    TextMatch,
    UnderPath,
)


@dataclass(slots=True)
class _Staged:
    directory: Path
    path: Path
    content_hash: str
    size: int
    mtime_ns: int | None


@dataclass(slots=True)
class _Prepared:
    record: dict[str, JSONValue]
    rows: list[IndexRow]
    staged: _Staged


@dataclass(slots=True)
class _FilteredDocument:
    id: DocumentId
    record: dict[str, Any]
    matches: tuple[Match, ...]


def _sort_id(value: DocumentId) -> tuple[bytes, bytes]:
    return value.namespace.encode(), value.doc_id.encode()


class MFS:
    @classmethod
    def open(
        cls,
        mfs_path: Path,
        processors: Sequence[Processor] = (),
        chunker: Chunker | None = None,
        embedder: Embedder | None = None,
        sync_policy: SyncPolicy | None = None,
    ) -> MFS:
        return cls(mfs_path, processors, chunker, embedder, sync_policy)

    def __init__(
        self,
        mfs_path: Path,
        processors: Sequence[Processor],
        chunker: Chunker | None,
        embedder: Embedder | None,
        sync_policy: SyncPolicy | None,
    ) -> None:
        if os.name != "posix" or sys.platform == "win32":
            raise InvalidConfiguration("MFS V1 only supports POSIX platforms")
        self._path = Path(mfs_path).expanduser().resolve()
        self._processors = validate_processors(processors)
        self._chunker = validate_chunker(chunker or DefaultChunker())
        self._embedder = validate_embedder(embedder)
        self._processor_descriptions = {
            id(processor): processor_description(processor) for processor in self._processors
        }
        self._processor_media_types = {
            id(processor): tuple(processor.media_types) for processor in self._processors
        }
        self._processor_suffixes = {
            id(processor): dict(processor.suffix_media_types) for processor in self._processors
        }
        self._chunker_description = chunker_description(self._chunker)
        self._embedder_space = self._embedder.embedding_space if self._embedder else None
        self._embedder_dimension = self._embedder.dimension if self._embedder else None
        self._sync_policy = self._validate_sync_policy(sync_policy or SyncPolicy())
        self._mutation_lock = threading.Lock()
        self._embedder_lock = threading.Lock()
        self._rwlock = ReadWriteLock()
        self._lifecycle = Lifecycle()
        self._catalog: Catalog
        self._index: ChunkIndex
        self._state: IndexState = "dirty"

        try:
            if self._path.exists() and not self._path.is_dir():
                raise CorruptState("mfs_path exists but is not a directory")
            self._path.mkdir(parents=True, exist_ok=True)
            existing = list(self._path.iterdir())
            initialize = not existing
            if existing and not (self._path / "catalog.sqlite").is_file():
                raise CorruptState("non-empty mfs_path has no recognizable V1 catalog")
            self._instance_lock = FileLock(self._path / "LOCK")
            try:
                self._instance_lock.acquire(timeout=0)
            except Timeout as error:
                raise InstanceLocked(f"MFS instance is already open: {self._path}") from error
            (self._path / "objects").mkdir(exist_ok=True)
            (self._path / "staging").mkdir(exist_ok=True)
            self._catalog = Catalog(self._path / "catalog.sqlite", initialize=initialize)
            self._index = ChunkIndex(self._path / "milvus.db")
            if initialize:
                self._initialize_index()
            else:
                self._load_index_state()
            self._recover_objects()
        except Exception:
            with contextlib.suppress(Exception):
                self._index.close()
            with contextlib.suppress(Exception):
                self._catalog.close()
            with contextlib.suppress(Exception):
                self._instance_lock.release()
            raise

    @staticmethod
    def _validate_sync_policy(policy: SyncPolicy) -> SyncPolicy:
        maximum = _runtime(policy.max_file_bytes)
        if maximum is not None and (
            isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 0
        ):
            raise InvalidConfiguration("max_file_bytes must be non-negative or None")
        for pattern in policy.exclude_globs:
            value = _runtime(pattern)
            if not isinstance(value, str) or not value or "\0" in value or "\\" in value:
                raise InvalidConfiguration("exclude_globs must be non-empty POSIX glob strings")
        return SyncPolicy(tuple(policy.exclude_globs), policy.max_file_bytes)

    def _initialize_index(self) -> None:
        dense = (
            dense_config(self._embedder_space, self._embedder_dimension)
            if self._embedder_space is not None and self._embedder_dimension is not None
            else None
        )
        config = index_config(cast(dict[str, object], self._chunker_description), dense)
        self._make_marker()
        try:
            self._index.recreate(dense_dimension=self._embedder_dimension)
            self._write_index_config(config)
            self._clear_marker()
            self._config = config
            self._state = "ready"
        except Exception:
            self._state = "dirty"
            raise

    def _load_index_state(self) -> None:
        marker = (self._path / "INDEX_DIRTY").exists()
        config_path = self._path / "index.json"
        try:
            raw = load_json(config_path.read_text("utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("index config is not an object")
            version = raw.get("version")
            if isinstance(version, int) and version > 1:
                raise SchemaVersionUnsupported(f"index schema version {version} is unsupported")
            if version != 1 or "chunker" not in raw or "bm25" not in raw or "dense" not in raw:
                raise ValueError("index config does not match V1")
            self._config = cast(dict[str, object], raw)
            dense_value = raw.get("dense")
            dimension = (
                _as_int(cast(dict[str, object], dense_value).get("dimension"), "dense dimension")
                if isinstance(dense_value, dict)
                else None
            )
            collection_valid = self._index.has_valid_collection(dense_dimension=dimension)
        except SchemaVersionUnsupported:
            raise
        except Exception:
            self._config = {}
            collection_valid = False
        if marker or not collection_valid or not self._config:
            self._state = "dirty"
            return
        expected_chunker = self._chunker_description
        dense = self._config.get("dense")
        mismatch = self._config.get("chunker") != expected_chunker
        if self._embedder_space is not None and self._embedder_dimension is not None:
            desired = dense_config(self._embedder_space, self._embedder_dimension)
            mismatch = mismatch or dense != desired
        self._state = "mismatch" if mismatch else "ready"

    def _write_index_config(self, value: dict[str, object]) -> None:
        temporary = self._path / f".index-{uuid.uuid4().hex}.tmp"
        try:
            with temporary.open("x", encoding="utf-8") as stream:
                stream.write(compact_json(value))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self._path / "index.json")
            self._fsync_directory(self._path)
        except OSError as error:
            raise StorageFailed(f"failed to persist index config: {error}") from error
        finally:
            with contextlib.suppress(OSError):
                temporary.unlink()

    def _make_marker(self) -> None:
        marker = self._path / "INDEX_DIRTY"
        try:
            descriptor = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                os.write(descriptor, b"1\n")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._fsync_directory(self._path)
        except FileExistsError:
            self._state = "dirty"
            raise IndexUnavailable("index is already dirty") from None
        except OSError as error:
            if marker.exists():
                self._state = "dirty"
            raise StorageFailed(f"failed to create dirty marker: {error}") from error

    def _clear_marker(self) -> None:
        try:
            (self._path / "INDEX_DIRTY").unlink()
            self._fsync_directory(self._path)
        except OSError as error:
            self._state = "dirty"
            raise StorageFailed(f"failed to clear dirty marker: {error}") from error

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _recover_objects(self) -> None:
        namespaces: dict[str, NamespaceInfo] = {}
        for namespace, record in self._catalog.list_namespaces():
            namespaces[namespace] = self._namespace_info(namespace, record)
        referenced: set[Path] = set()
        for namespace, doc_id, record in self._catalog.list_documents():
            try:
                if record.get("version") != 1:
                    raise ValueError("document version is not 1")
                media_type = record["media_type"]
                if (
                    not isinstance(media_type, str)
                    or normalized_media_type(media_type) != media_type
                ):
                    raise ValueError("media type is invalid")
                for field in ("content_hash", "snapshot_id"):
                    value = record[field]
                    if (
                        not isinstance(value, str)
                        or len(value) != 64
                        or any(character not in "0123456789abcdef" for character in value)
                    ):
                        raise ValueError(f"{field} is not lowercase BLAKE3 hex")
                processor = record["processor"]
                if not isinstance(processor, dict):
                    raise ValueError("processor description is invalid")
                processor_data = cast(dict[str, object], processor)
                if not processor_data.get("id") or not processor_data.get("version"):
                    raise ValueError("processor id/version is empty")
                canonical_json(processor_data.get("options"))
                text = record["text"]
                if not isinstance(text, str):
                    raise ValueError("text is not a string")
                source_map = self._source_map(record)
                validate_source_map(source_map, len(text.encode("utf-8", errors="strict")))
                source = record["source"]
                if not isinstance(source, dict):
                    raise ValueError("source is invalid")
                source_data = cast(dict[str, object], source)
                size = source_data["size"]
                if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                    raise ValueError("source size is invalid")
                object_name = source_data["object"]
                namespace_info = namespaces[namespace]
                if namespace_info.kind == "internal" and source_data.get("mtime_ns") is not None:
                    raise ValueError("internal source must not persist mtime")
                if namespace_info.kind == "external" and object_name is not None:
                    raise ValueError("external source must not reference an object")
                if namespace_info.kind == "internal" and object_name is None:
                    raise ValueError("internal source must reference an object")
                if namespace_info.kind == "external":
                    mtime_ns = source_data.get("mtime_ns")
                    if isinstance(mtime_ns, bool) or not isinstance(mtime_ns, int):
                        raise ValueError("external source mtime is invalid")
                if object_name is None:
                    continue
                if not isinstance(object_name, str):
                    raise ValueError("object path is not a string")
                object_path = (self._path / object_name).resolve(strict=True)
                objects_root = (self._path / "objects").resolve(strict=True)
                if object_path.parent != objects_root or not object_path.is_file():
                    raise ValueError("object path is missing or outside objects")
                referenced.add(object_path)
            except Exception as error:
                raise CorruptState(
                    f"invalid document record {namespace}/{doc_id}: {error}"
                ) from error
        for path in (self._path / "objects").iterdir():
            if path.is_file() and path.resolve() not in referenced:
                with contextlib.suppress(OSError):
                    path.unlink()
        for path in (self._path / "staging").iterdir():
            with contextlib.suppress(OSError):
                if path.is_dir() and not path.is_symlink():
                    shutil.rmtree(path)
                else:
                    path.unlink()

    def _ready(self) -> None:
        if self._state != "ready":
            raise IndexUnavailable(f"index state is {self._state}; call reindex()")

    @contextlib.contextmanager
    def _call(self) -> Generator[None, None, None]:
        with self._lifecycle.call():
            yield

    def close(self) -> None:
        if not self._lifecycle.begin_close():
            return
        error: Exception | None = None
        try:
            try:
                self._index.close()
            except Exception as close_error:
                error = close_error
            try:
                self._catalog.close()
            except Exception as close_error:
                error = error or close_error
            self._instance_lock.release()
        finally:
            self._lifecycle.finish_close()
        if error is not None:
            raise error

    def create_namespace(
        self, namespace: str, kind: NamespaceKind, root: Path | None = None
    ) -> NamespaceInfo:
        with self._call(), self._mutation_lock:
            validate_namespace(namespace)
            if kind not in ("internal", "external"):
                raise InvalidConfiguration("namespace kind must be 'internal' or 'external'")
            if kind == "internal":
                if root is not None:
                    raise InvalidConfiguration("internal namespace must not have a root")
                resolved_root = None
            else:
                if root is None:
                    raise InvalidConfiguration("external namespace requires a root")
                try:
                    resolved_root = Path(root).expanduser().resolve(strict=True)
                except OSError as error:
                    raise SourceUnavailable(f"external root is unavailable: {error}") from error
                if not resolved_root.is_dir() or not os.access(resolved_root, os.R_OK):
                    raise SourceUnavailable("external root must be a readable directory")
                if _paths_overlap(self._path, resolved_root):
                    raise RootOverlap("external root and mfs_path must not overlap")
            existing = self._catalog.get_namespace(namespace)
            expected = {
                "version": 1,
                "kind": kind,
                "root": str(resolved_root) if resolved_root is not None else None,
            }
            if existing is not None:
                if existing == expected:
                    return self._namespace_info(namespace, existing)
                raise NamespaceConflict(
                    f"namespace {namespace!r} already has a different kind or root"
                )
            with self._rwlock.write():
                self._catalog.put_namespace(namespace, cast(dict[str, JSONValue], expected))
            return NamespaceInfo(namespace, kind, resolved_root)

    def get_namespace(self, namespace: str) -> NamespaceInfo:
        with self._call(), self._rwlock.read():
            validate_namespace(namespace)
            record = self._catalog.get_namespace(namespace)
            if record is None:
                raise NamespaceNotFound(f"namespace {namespace!r} does not exist")
            return self._namespace_info(namespace, record)

    def list_namespaces(self) -> tuple[NamespaceInfo, ...]:
        with self._call(), self._rwlock.read():
            values = [
                self._namespace_info(name, record)
                for name, record in self._catalog.list_namespaces()
            ]
            return tuple(sorted(values, key=lambda item: item.namespace.encode()))

    @staticmethod
    def _namespace_info(namespace: str, record: dict[str, Any]) -> NamespaceInfo:
        try:
            kind = cast(NamespaceKind, record["kind"])
            root = Path(record["root"]) if kind == "external" else None
            if kind not in ("internal", "external") or (kind == "external") != (root is not None):
                raise ValueError("invalid kind/root")
            return NamespaceInfo(namespace, kind, root)
        except Exception as error:
            raise CorruptState(f"invalid namespace record {namespace!r}") from error

    def drop_namespace(self, namespace: str) -> DropReport:
        with self._call(), self._mutation_lock:
            validate_namespace(namespace)
            record = self._catalog.get_namespace(namespace)
            if record is None:
                return DropReport(namespace, False, self._state == "ready")
            documents = self._catalog.list_namespace_documents(namespace)
            if not documents:
                with self._rwlock.write():
                    self._catalog.delete_namespace(namespace)
                return DropReport(namespace, True, self._state == "ready")
            self._ready()
            object_paths = [self._object_path(item) for _, item in documents]
            with self._rwlock.write():
                self._make_marker()
                try:
                    self._index.delete_namespace(namespace)
                    self._catalog.delete_namespace(namespace)
                except Exception:
                    self._state = "dirty"
                    raise
                ready = self._cleanup_marker_after_commit()
            for object_path in object_paths:
                if object_path is not None:
                    with contextlib.suppress(OSError):
                        object_path.unlink()
            return DropReport(namespace, True, ready)

    def status(self) -> Status:
        with self._call(), self._rwlock.read():
            dense = self._dense_config()
            available = (
                dense is not None
                and self._embedder is not None
                and dense.get("embedding_space") == self._embedder_space
                and dense.get("dimension") == self._embedder_dimension
                and self._state != "mismatch"
            )
            return Status(
                namespace_count=self._catalog.namespace_count(),
                document_count=self._catalog.document_count(),
                index_state=self._state,
                dense_enabled=dense is not None,
                dense_available=available,
            )

    def _dense_config(self) -> dict[str, object] | None:
        dense = self._config.get("dense")
        return cast(dict[str, object], dense) if isinstance(dense, dict) else None

    def _cleanup_marker_after_commit(self) -> bool:
        try:
            self._clear_marker()
        except StorageFailed:
            self._state = "dirty"
            return False
        self._state = "ready"
        return True

    def upsert(
        self,
        namespace: str,
        doc_id: str,
        data: Path | bytes,
        media_type: str | None = None,
    ) -> MutationReport:
        with self._call(), self._mutation_lock:
            validate_namespace(namespace)
            validate_internal_id(doc_id)
            info = self._required_namespace(namespace)
            if info.kind != "internal":
                raise WrongNamespaceKind("upsert is only valid for internal namespaces")
            self._ready()
            document_id = DocumentId(namespace, doc_id)
            staged = (
                self._stage_bytes(data) if isinstance(data, bytes) else self._stage_path(Path(data))
            )
            try:
                previous = self._catalog.get_document(namespace, doc_id)
                prepared = self._prepare(
                    document_id,
                    staged,
                    previous,
                    explicit_media_type=media_type,
                    fallback_path=data if isinstance(data, Path) else None,
                    external=False,
                )
                if prepared is None:
                    return MutationReport(document_id, "unchanged", True)
                old_object = self._object_path(previous) if previous is not None else None
                object_relative = f"objects/{uuid.uuid4().hex}"
                object_path = self._path / object_relative
                try:
                    os.replace(staged.path, object_path)
                    with object_path.open("rb") as stream:
                        os.fsync(stream.fileno())
                    self._fsync_directory(object_path.parent)
                except OSError as error:
                    raise StorageFailed(f"failed to store internal object: {error}") from error
                source = cast(dict[str, JSONValue], prepared.record["source"])
                source["object"] = object_relative
                try:
                    ready = self._publish_replace(document_id, prepared.record, prepared.rows)
                except Exception:
                    if self._state == "ready":
                        with contextlib.suppress(OSError):
                            object_path.unlink()
                    raise
                if old_object is not None:
                    with contextlib.suppress(OSError):
                        old_object.unlink()
                outcome: Literal["added", "updated"] = "added" if previous is None else "updated"
                return MutationReport(document_id, outcome, ready)
            finally:
                self._remove_staging(staged.directory)

    def remove(self, namespace: str, doc_id: str) -> MutationReport:
        with self._call(), self._mutation_lock:
            validate_namespace(namespace)
            validate_internal_id(doc_id)
            info = self._required_namespace(namespace)
            if info.kind != "internal":
                raise WrongNamespaceKind("remove is only valid for internal namespaces")
            self._ready()
            document_id = DocumentId(namespace, doc_id)
            previous = self._catalog.get_document(namespace, doc_id)
            if previous is None:
                return MutationReport(document_id, "not_found", True)
            old_object = self._object_path(previous)
            ready = self._publish_delete(document_id)
            if old_object is not None:
                with contextlib.suppress(OSError):
                    old_object.unlink()
            return MutationReport(document_id, "removed", ready)

    def _required_namespace(self, namespace: str) -> NamespaceInfo:
        record = self._catalog.get_namespace(namespace)
        if record is None:
            raise NamespaceNotFound(f"namespace {namespace!r} does not exist")
        return self._namespace_info(namespace, record)

    def _stage_bytes(self, data: bytes) -> _Staged:
        directory = self._new_staging()
        path = directory / "source"
        try:
            with path.open("xb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            return _Staged(directory, path, blake3.blake3(data).hexdigest(), len(data), None)
        except OSError as error:
            self._remove_staging(directory)
            raise StorageFailed(f"failed to stage bytes: {error}") from error

    def _stage_path(self, source: Path) -> _Staged:
        directory = self._new_staging()
        staged_path = directory / "source"
        try:
            for attempt in range(2):
                try:
                    source_lstat = source.lstat()
                    if stat.S_ISLNK(source_lstat.st_mode) or not stat.S_ISREG(source_lstat.st_mode):
                        raise SourceUnavailable("source must be a regular file and not a symlink")
                    digest = blake3.blake3()
                    with source.open("rb") as input_stream, staged_path.open("wb") as output_stream:
                        before = os.fstat(input_stream.fileno())
                        while block := input_stream.read(1024 * 1024):
                            output_stream.write(block)
                            digest.update(block)
                        output_stream.flush()
                        os.fsync(output_stream.fileno())
                        after = os.fstat(input_stream.fileno())
                    path_after = source.lstat()
                    identity_before = (
                        before.st_dev,
                        before.st_ino,
                        before.st_size,
                        before.st_mtime_ns,
                    )
                    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                    path_identity = (
                        path_after.st_dev,
                        path_after.st_ino,
                        path_after.st_size,
                        path_after.st_mtime_ns,
                    )
                    if identity_before == identity_after == path_identity:
                        return _Staged(
                            directory,
                            staged_path,
                            digest.hexdigest(),
                            before.st_size,
                            before.st_mtime_ns,
                        )
                except (FileNotFoundError, PermissionError, OSError) as error:
                    if attempt == 1:
                        raise SourceUnavailable(f"source is unavailable: {error}") from error
                if attempt == 1:
                    raise SourceChanged("source changed during both stable-read attempts")
            raise AssertionError("unreachable")
        except Exception:
            self._remove_staging(directory)
            raise

    def _new_staging(self) -> Path:
        directory = self._path / "staging" / uuid.uuid4().hex
        try:
            directory.mkdir()
            return directory
        except OSError as error:
            raise StorageFailed(f"failed to create staging directory: {error}") from error

    @staticmethod
    def _remove_staging(directory: Path) -> None:
        with contextlib.suppress(OSError):
            shutil.rmtree(directory)

    def _prepare(
        self,
        document_id: DocumentId,
        staged: _Staged,
        previous: dict[str, Any] | None,
        *,
        explicit_media_type: str | None,
        fallback_path: Path | None,
        external: bool,
    ) -> _Prepared | None:
        media_type, processor = self._select_processor(
            document_id.doc_id, staged.path, explicit_media_type, fallback_path
        )
        description = self._processor_descriptions[id(processor)]
        if (
            previous is not None
            and previous.get("content_hash") == staged.content_hash
            and previous.get("media_type") == media_type
            and previous.get("processor") == description
        ):
            return None
        try:
            processed = validate_processed(processor.process(staged.path, media_type))
            ranges = validate_chunk_ranges(
                processed.text, self._chunker.chunk(processed.text, processed.source_map)
            )
        except MFSError:
            raise
        except Exception as error:
            raise ProcessingFailed(
                f"processing failed for {document_id.namespace}/{document_id.doc_id}: {error}",
                document_id=document_id,
            ) from error
        encoded = processed.text.encode("utf-8")
        chunk_texts = [encoded[item.text_start : item.text_end].decode("utf-8") for item in ranges]
        vectors = self._embed_documents(chunk_texts) if self._dense_config() is not None else None
        snapshot_input = {
            "content_hash": staged.content_hash,
            "media_type": media_type,
            "processor": description,
        }
        snapshot_id = blake3.blake3(canonical_json(snapshot_input)).hexdigest()
        record: dict[str, JSONValue] = {
            "version": 1,
            "media_type": media_type,
            "content_hash": staged.content_hash,
            "snapshot_id": snapshot_id,
            "processor": description,
            "text": processed.text,
            "source_map": self._source_map_json(processed.source_map),
            "source": {
                "size": staged.size,
                "mtime_ns": staged.mtime_ns if external else None,
                "object": None,
            },
        }
        rows: list[IndexRow] = []
        for index, chunk_range in enumerate(ranges):
            rows.append(
                IndexRow(
                    namespace=document_id.namespace,
                    doc_id=document_id.doc_id,
                    ordinal=index,
                    text=chunk_texts[index],
                    text_start=chunk_range.text_start,
                    text_end=chunk_range.text_end,
                    dense_vector=vectors[index] if vectors is not None else [],
                )
            )
        return _Prepared(record, rows, staged)

    def _select_processor(
        self,
        doc_id: str,
        staged_path: Path,
        explicit_media_type: str | None,
        fallback_path: Path | None,
    ) -> tuple[str, Processor]:
        by_media = {
            media_type: processor
            for processor in self._processors
            for media_type in self._processor_media_types[id(processor)]
        }
        by_suffix = {
            suffix: (media_type, processor)
            for processor in self._processors
            for suffix, media_type in self._processor_suffixes[id(processor)].items()
        }
        if explicit_media_type is not None:
            media_type = normalized_media_type(explicit_media_type)
            processor = by_media.get(media_type)
            if processor is None:
                raise UnsupportedMediaType(f"no Processor handles {media_type}")
            return media_type, processor
        suffix_match = by_suffix.get(suffix_for(doc_id))
        if suffix_match is None and fallback_path is not None:
            suffix_match = by_suffix.get(suffix_for(fallback_path))
        if suffix_match is not None:
            return suffix_match
        try:
            head = staged_path.read_bytes()[: 64 * 1024]
            sniffed: list[tuple[str, Processor]] = []
            for processor in self._processors:
                media_type = processor.sniff(head)
                if media_type is None:
                    continue
                if media_type not in self._processor_media_types[id(processor)]:
                    processor_id = self._processor_descriptions[id(processor)]["id"]
                    raise InvalidConfiguration(
                        f"Processor {processor_id!r} sniff returned unowned media type "
                        f"{media_type!r}"
                    )
                sniffed.append((media_type, processor))
            if len(sniffed) > 1:
                raise InvalidConfiguration(
                    "multiple Processors matched the same source by sniffing"
                )
            if sniffed:
                return sniffed[0]
        except MFSError:
            raise
        except Exception as error:
            raise ProcessingFailed(f"Processor sniff failed: {error}") from error
        raise UnsupportedMediaType(f"no Processor handles {doc_id!r}")

    def _embed_documents(
        self, texts: Sequence[str], *, target: Embedder | None = None
    ) -> list[list[float]]:
        if not texts:
            return []
        embedder = target or self._matching_embedder()
        result: list[list[float]] = []
        for start in range(0, len(texts), 128):
            batch = texts[start : start + 128]
            try:
                with self._embedder_lock:
                    vectors = embedder.embed_documents(batch)
                dimension = self._embedder_dimension
                if dimension is None:
                    raise CapabilityUnavailable("active operation requires an Embedder")
                result.extend(self._validate_vectors(vectors, len(batch), dimension))
            except MFSError:
                raise
            except Exception as error:
                raise EmbeddingFailed(f"document embedding failed: {error}") from error
        return result

    def _embed_query(self, text: str) -> list[float]:
        embedder = self._matching_embedder()
        try:
            with self._embedder_lock:
                vector = embedder.embed_query(text)
            dimension = self._embedder_dimension
            if dimension is None:
                raise CapabilityUnavailable("active operation requires an Embedder")
            return self._validate_vectors([vector], 1, dimension)[0]
        except MFSError:
            raise
        except Exception as error:
            raise EmbeddingFailed(f"query embedding failed: {error}") from error

    def _matching_embedder(self) -> Embedder:
        dense = self._dense_config()
        if dense is None or self._embedder is None:
            raise CapabilityUnavailable("active operation requires an Embedder")
        if (
            dense.get("embedding_space") != self._embedder_space
            or dense.get("dimension") != self._embedder_dimension
        ):
            raise CapabilityUnavailable("provided Embedder does not match the active index")
        return self._embedder

    @staticmethod
    def _validate_vectors(
        vectors: Sequence[Sequence[float]], count: int, dimension: int
    ) -> list[list[float]]:
        if len(vectors) != count:
            raise EmbeddingFailed(f"Embedder returned {len(vectors)} vectors; expected {count}")
        result: list[list[float]] = []
        for vector in vectors:
            if len(vector) != dimension:
                raise EmbeddingFailed(
                    f"Embedder returned dimension {len(vector)}; expected {dimension}"
                )
            converted = [float(item) for item in vector]
            if not all(math.isfinite(item) for item in converted):
                raise EmbeddingFailed("Embedder returned non-finite values")
            result.append(converted)
        return result

    def _publish_replace(
        self, document_id: DocumentId, record: dict[str, JSONValue], rows: Sequence[IndexRow]
    ) -> bool:
        with self._rwlock.write():
            self._make_marker()
            try:
                self._index.replace(document_id, rows)
                self._catalog.put_document(document_id.namespace, document_id.doc_id, record)
            except Exception:
                self._state = "dirty"
                raise
            return self._cleanup_marker_after_commit()

    def _publish_delete(self, document_id: DocumentId) -> bool:
        with self._rwlock.write():
            self._make_marker()
            try:
                self._index.delete_document(document_id)
                self._index.flush()
                self._catalog.delete_document(document_id.namespace, document_id.doc_id)
            except Exception:
                self._state = "dirty"
                raise
            return self._cleanup_marker_after_commit()

    def _object_path(self, record: dict[str, Any] | None) -> Path | None:
        if record is None:
            return None
        try:
            value = record["source"]["object"]
            if value is None:
                return None
            candidate = self._path / str(value)
            if candidate.parent.resolve() != (self._path / "objects").resolve():
                raise ValueError("object path escapes objects")
            return candidate
        except Exception as error:
            raise CorruptState("invalid internal object reference") from error

    @staticmethod
    def _source_map_json(source_map: SourceMap) -> dict[str, JSONValue]:
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

    @staticmethod
    def _source_map(record: dict[str, Any]) -> SourceMap:
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

    def _document(self, document_id: DocumentId, record: dict[str, Any]) -> Document:
        object_path = self._object_path(record)
        try:
            original = object_path.read_bytes() if object_path is not None else None
            return Document(
                id=document_id,
                snapshot_id=str(record["snapshot_id"]),
                media_type=str(record["media_type"]),
                text=str(record["text"]),
                source_map=self._source_map(record),
                original=original,
            )
        except OSError as error:
            raise CorruptState(f"internal object is unavailable: {error}") from error

    @staticmethod
    def _source_location(source_map: SourceMap, start: int, end: int) -> SourceLocation:
        sources: list[JSONValue] = []
        seen: set[bytes] = set()
        for span in source_map.spans:
            if span.text_start < end and start < span.text_end:
                encoded = canonical_json(span.source)
                if encoded not in seen:
                    sources.append(copy_json(span.source))
                    seen.add(encoded)
        return SourceLocation(1, tuple(sources))

    def _chunk(self, row: dict[str, object] | SearchHit, record: dict[str, Any]) -> Chunk:
        document_id = DocumentId(str(row["namespace"]), str(row["doc_id"]))
        start = _as_int(row["text_start"], "chunk text_start")
        end = _as_int(row["text_end"], "chunk text_end")
        return Chunk(
            document_id=document_id,
            snapshot_id=str(record["snapshot_id"]),
            ordinal=_as_int(row["ordinal"], "chunk ordinal"),
            text=str(row["text"]),
            text_start=start,
            text_end=end,
            source_location=self._source_location(self._source_map(record), start, end),
        )

    def query(
        self,
        filters: Sequence[Filter] = (),
        select: Select = "doc_id",
        limit: int | None = None,
    ) -> QueryResult[Any]:
        with self._call(), self._rwlock.read():
            self._validate_query_options(select, limit, search=False)
            documents = self._filter_documents(filters)
            items: list[QueryItem[Any]] = []
            if select == "doc_id":
                items = [QueryItem(item.id, item.matches) for item in documents]
            elif select == "doc":
                items = [
                    QueryItem(self._document(item.id, item.record), item.matches)
                    for item in documents
                ]
            else:
                self._ready()
                wanted = {item.id: item for item in documents}
                for row in self._sorted_rows(self._index.scan()):
                    document_id = DocumentId(str(row["namespace"]), str(row["doc_id"]))
                    filtered = wanted.get(document_id)
                    if filtered is None:
                        continue
                    chunk_matches = tuple(
                        match
                        for match in filtered.matches
                        if match.text_start < _as_int(row["text_end"], "chunk text_end")
                        and _as_int(row["text_start"], "chunk text_start") < match.text_end
                    )
                    if filtered.matches and not chunk_matches:
                        continue
                    items.append(QueryItem(self._chunk(row, filtered.record), chunk_matches))
            truncated = limit is not None and len(items) > limit
            if limit is not None:
                items = items[:limit]
            return QueryResult(tuple(items), truncated)

    @staticmethod
    def _validate_query_options(select: str, limit: int | None, *, search: bool) -> None:
        if select not in ("doc_id", "chunk", "doc"):
            raise InvalidQuery(f"invalid select: {select!r}")
        if search:
            if limit is None or not 1 <= limit <= 1000:
                raise InvalidQuery("search limit must be between 1 and 1000")
        elif limit is not None and not 1 <= limit <= 100000:
            raise InvalidQuery("query limit must be None or between 1 and 100000")

    @staticmethod
    def _sorted_rows(rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
        return sorted(
            rows,
            key=lambda row: (
                str(row["namespace"]).encode(),
                str(row["doc_id"]).encode(),
                _as_int(row["ordinal"], "chunk ordinal"),
            ),
        )

    def _filter_documents(self, filters: Sequence[Filter]) -> list[_FilteredDocument]:
        records = {
            DocumentId(namespace, doc_id): record
            for namespace, doc_id, record in self._catalog.list_documents()
        }
        candidates = set(records)
        text_filters: list[TextMatch] = []
        for item in filters:
            if isinstance(item, ByNamespace):
                if not item.namespaces:
                    raise InvalidFilter("ByNamespace values must not be empty")
                names = set(item.namespaces)
                for namespace in names:
                    validate_namespace(namespace)
                    self._required_namespace(namespace)
                candidates &= {
                    document_id for document_id in candidates if document_id.namespace in names
                }
            elif isinstance(item, ByDocumentId):
                if not item.ids:
                    raise InvalidFilter("ByDocumentId values must not be empty")
                ids = set(item.ids)
                for document_id in ids:
                    info = self._required_namespace(document_id.namespace)
                    if info.kind == "internal":
                        validate_internal_id(document_id.doc_id)
                    else:
                        validate_external_path(document_id.doc_id, allow_root=False)
                candidates &= ids
            elif isinstance(item, UnderPath):
                info = self._required_namespace(item.namespace)
                if info.kind != "external":
                    raise InvalidFilter("UnderPath requires an external namespace")
                path = validate_external_path(item.path, allow_root=True)
                candidates &= {
                    document_id
                    for document_id in candidates
                    if document_id.namespace == item.namespace
                    and (
                        path == "."
                        or document_id.doc_id == path
                        or document_id.doc_id.startswith(path + "/")
                    )
                }
            elif isinstance(item, TextMatch):
                text_filters.append(item)
            else:
                raise InvalidFilter(f"unsupported Filter type: {type(item).__name__}")
        result: list[_FilteredDocument] = []
        for document_id in sorted(candidates, key=_sort_id):
            record = records[document_id]
            matches: list[tuple[int, int]] = []
            accepted = True
            for text_filter in text_filters:
                current = self._text_matches(str(record["text"]), text_filter)
                if not current:
                    accepted = False
                    break
                matches.extend(current)
            if not accepted:
                continue
            merged = _merge_ranges(matches)
            source_map = self._source_map(record)
            result.append(
                _FilteredDocument(
                    document_id,
                    record,
                    tuple(
                        Match(start, end, self._source_location(source_map, start, end))
                        for start, end in merged
                    ),
                )
            )
        return result

    @staticmethod
    def _text_matches(text: str, text_filter: TextMatch) -> list[tuple[int, int]]:
        value = _runtime(text_filter.pattern)
        if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 16 * 1024:
            raise InvalidFilter("TextMatch pattern must be 1..16384 UTF-8 bytes")
        pattern = value
        try:
            character_ranges = regex_ranges(
                text,
                pattern,
                regex=text_filter.regex,
                case_sensitive=text_filter.case_sensitive,
            )
        except Exception as error:
            raise InvalidPattern(f"invalid RE2 pattern: {error}") from error
        offsets = [0]
        for character in text:
            offsets.append(offsets[-1] + len(character.encode("utf-8")))
        return [(offsets[start], offsets[end]) for start, end in character_ranges]

    def search(
        self,
        text: str,
        filters: Sequence[Filter] = (),
        mode: SearchMode = "hybrid",
        select: Select = "chunk",
        limit: int = 10,
    ) -> SearchResult[Any]:
        with self._call(), self._rwlock.read():
            text_value = _runtime(text)
            if (
                not isinstance(text_value, str)
                or not text_value
                or len(text_value.encode("utf-8")) > 64 * 1024
            ):
                raise InvalidQuery("search text must be 1..65536 UTF-8 bytes")
            text = text_value
            if mode not in ("bm25", "vector", "hybrid"):
                raise InvalidQuery(f"invalid search mode: {mode!r}")
            self._validate_query_options(select, limit, search=True)
            self._ready()
            documents = self._filter_documents(filters)
            by_id = {item.id: item for item in documents}
            restricted = [item.id for item in documents] if filters else None
            candidate_limit = (
                1000 if select in ("doc_id", "doc") else min(1000, max(100, limit * 10))
            )
            channels: list[list[SearchHit]] = []
            backend_truncated = False
            if mode in ("bm25", "hybrid"):
                hits, more = self._index.search(
                    text, mode="bm25", documents=restricted, limit=candidate_limit
                )
                channels.append(hits)
                backend_truncated |= more
            if mode in ("vector", "hybrid"):
                vector = self._embed_query(text)
                hits, more = self._index.search(
                    vector, mode="vector", documents=restricted, limit=candidate_limit
                )
                channels.append(hits)
                backend_truncated |= more
            ranked = _rrf(channels) if mode == "hybrid" else channels[0]
            ranked = [hit for hit in ranked if DocumentId(hit["namespace"], hit["doc_id"]) in by_id]
            items: list[SearchItem[Any]] = []
            if select == "chunk":
                for hit in ranked:
                    document_id = DocumentId(hit["namespace"], hit["doc_id"])
                    filtered = by_id[document_id]
                    items.append(
                        SearchItem(
                            self._chunk(hit, filtered.record), hit["score"], filtered.matches
                        )
                    )
            else:
                seen: set[DocumentId] = set()
                for hit in ranked:
                    document_id = DocumentId(hit["namespace"], hit["doc_id"])
                    if document_id in seen:
                        continue
                    seen.add(document_id)
                    filtered = by_id[document_id]
                    value: Any = (
                        document_id
                        if select == "doc_id"
                        else self._document(document_id, filtered.record)
                    )
                    items.append(SearchItem(value, hit["score"], filtered.matches))
            truncated = backend_truncated or len(items) > limit
            return SearchResult(tuple(items[:limit]), truncated)

    def reindex(self) -> ReindexReport:
        with self._call(), self._mutation_lock, self._rwlock.write():
            current_dense = self._dense_config()
            if self._embedder_space is not None and self._embedder_dimension is not None:
                target_dense = dense_config(self._embedder_space, self._embedder_dimension)
            else:
                target_dense = current_dense
            if not (self._path / "INDEX_DIRTY").exists():
                self._make_marker()
            try:
                if target_dense is not None:
                    if self._embedder is None:
                        raise CapabilityUnavailable(
                            "reindexing a dense index requires its Embedder"
                        )
                    if (
                        target_dense.get("embedding_space") != self._embedder_space
                        or target_dense.get("dimension") != self._embedder_dimension
                    ):
                        raise CapabilityUnavailable(
                            "provided Embedder does not match the target index"
                        )
                documents = self._catalog.list_documents()
                rows: list[IndexRow] = []
                chunk_texts: list[str] = []
                row_metadata: list[tuple[DocumentId, int, int, int, str]] = []
                for namespace, doc_id, record in documents:
                    text = str(record["text"])
                    source_map = self._source_map(record)
                    try:
                        ranges = validate_chunk_ranges(text, self._chunker.chunk(text, source_map))
                    except MFSError:
                        raise
                    except Exception as error:
                        raise ProcessingFailed(f"Chunker failed during reindex: {error}") from error
                    encoded = text.encode("utf-8")
                    for ordinal, chunk_range in enumerate(ranges):
                        chunk_text = encoded[chunk_range.text_start : chunk_range.text_end].decode(
                            "utf-8"
                        )
                        chunk_texts.append(chunk_text)
                        row_metadata.append(
                            (
                                DocumentId(namespace, doc_id),
                                ordinal,
                                chunk_range.text_start,
                                chunk_range.text_end,
                                chunk_text,
                            )
                        )
                vectors = (
                    self._embed_documents(chunk_texts, target=self._embedder)
                    if target_dense is not None
                    else None
                )
                for index, (document_id, ordinal, start, end, chunk_text) in enumerate(
                    row_metadata
                ):
                    rows.append(
                        IndexRow(
                            namespace=document_id.namespace,
                            doc_id=document_id.doc_id,
                            ordinal=ordinal,
                            text=chunk_text,
                            text_start=start,
                            text_end=end,
                            dense_vector=vectors[index] if vectors is not None else [],
                        )
                    )
                dimension = (
                    _as_int(target_dense["dimension"], "dense dimension")
                    if target_dense is not None
                    else None
                )
                self._index.recreate(dense_dimension=dimension)
                self._index.insert(rows)
                self._index.flush()
                if len(self._index.scan()) != len(rows):
                    raise IndexFailed("reindex row count validation failed")
                config = index_config(
                    cast(dict[str, object], self._chunker_description), target_dense
                )
                self._write_index_config(config)
                self._config = config
                self._clear_marker()
                self._state = "ready"
            except Exception:
                self._state = "dirty"
                raise
            return ReindexReport(len(documents), len(rows), target_dense is not None)

    def sync(self, namespace: str, path: str = ".") -> SyncReport:
        with self._call(), self._mutation_lock:
            validate_namespace(namespace)
            requested = validate_external_path(path, allow_root=True)
            info = self._required_namespace(namespace)
            if info.kind != "external" or info.root is None:
                raise WrongNamespaceKind("sync is only valid for external namespaces")
            self._ready()
            root = info.root
            changed: list[DocumentId] = []
            removed: list[DocumentId] = []
            failed: dict[tuple[str, str], SyncFailure] = {}
            skipped: dict[tuple[str, str], SyncSkipped] = {}
            seen: set[str] = set()
            nonmembers: set[str] = set()
            complete = True
            stop = False

            def failure(relative: str, error: Exception) -> None:
                if isinstance(error, MFSError):
                    code, message = error.code, error.message
                else:
                    code, message = "SourceUnavailable", str(error)
                failed[(relative, code)] = SyncFailure(relative, code, message)

            def skip(relative: str, reason: str) -> None:
                skipped[(relative, reason)] = SyncSkipped(relative, cast(Any, reason))

            def process_file(
                source: Path, relative: str, source_stat: os.stat_result, force: bool
            ) -> None:
                nonlocal complete, stop
                seen.add(relative)
                previous = self._catalog.get_document(namespace, relative)
                if not force and self._stat_unchanged(relative, source_stat, previous):
                    return
                staged: _Staged | None = None
                try:
                    staged = self._stage_path(source)
                    prepared = self._prepare(
                        DocumentId(namespace, relative),
                        staged,
                        previous,
                        explicit_media_type=None,
                        fallback_path=None,
                        external=True,
                    )
                    if prepared is None:
                        return
                    ready = self._publish_replace(
                        DocumentId(namespace, relative), prepared.record, prepared.rows
                    )
                    changed.append(DocumentId(namespace, relative))
                    if not ready:
                        complete = False
                        stop = True
                except UnsupportedMediaType:
                    skip(relative, "unsupported_media_type")
                except (
                    CapabilityUnavailable,
                    EmbeddingFailed,
                    ProcessingFailed,
                    SourceChanged,
                    SourceUnavailable,
                ) as error:
                    failure(relative, error)
                except (IndexFailed, IndexUnavailable, StorageFailed) as error:
                    failure(relative, error)
                    if self._state != "ready":
                        complete = False
                        stop = True
                finally:
                    if staged is not None:
                        self._remove_staging(staged.directory)

            def walk(directory: Path, relative_dir: str) -> None:
                nonlocal complete
                try:
                    with os.scandir(directory) as iterator:
                        entries = sorted(iterator, key=lambda item: item.name.encode())
                except OSError as error:
                    complete = False
                    failure(
                        relative_dir or ".", SourceUnavailable(str(error), path=relative_dir or ".")
                    )
                    return
                for entry in entries:
                    if stop:
                        return
                    relative = f"{relative_dir}/{entry.name}" if relative_dir else entry.name
                    try:
                        entry_stat = entry.stat(follow_symlinks=False)
                    except OSError as error:
                        complete = False
                        failure(relative, SourceUnavailable(str(error), path=relative))
                        continue
                    if self._excluded(relative):
                        nonmembers.add(relative)
                        skip(relative, "excluded")
                    elif stat.S_ISLNK(entry_stat.st_mode):
                        nonmembers.add(relative)
                        skip(relative, "symlink")
                    elif stat.S_ISDIR(entry_stat.st_mode):
                        walk(Path(entry.path), relative)
                    elif stat.S_ISREG(entry_stat.st_mode):
                        maximum = self._sync_policy.max_file_bytes
                        if maximum is not None and entry_stat.st_size > maximum:
                            nonmembers.add(relative)
                            skip(relative, "too_large")
                        else:
                            process_file(Path(entry.path), relative, entry_stat, False)
                    else:
                        nonmembers.add(relative)
                        skip(relative, "special_file")

            try:
                target, actual_relative = self._observe_target(root, requested)
                if target is None:
                    # A missing target under an observable parent is a complete observation.
                    pass
                else:
                    target_stat = target.lstat()
                    report_path = actual_relative if requested != "." else "."
                    if requested != "." and self._excluded(actual_relative):
                        nonmembers.add(actual_relative)
                        skip(actual_relative, "excluded")
                    elif stat.S_ISLNK(target_stat.st_mode):
                        nonmembers.add(actual_relative)
                        skip(actual_relative, "symlink")
                    elif stat.S_ISREG(target_stat.st_mode):
                        maximum = self._sync_policy.max_file_bytes
                        if maximum is not None and target_stat.st_size > maximum:
                            nonmembers.add(actual_relative)
                            skip(actual_relative, "too_large")
                        else:
                            process_file(target, actual_relative, target_stat, True)
                    elif stat.S_ISDIR(target_stat.st_mode):
                        walk(target, "" if requested == "." else actual_relative)
                    else:
                        nonmembers.add(actual_relative)
                        skip(actual_relative, "special_file")
                    del report_path
            except (InvalidPath, SourceUnavailable) as error:
                complete = False
                failure(requested, error)

            if not stop:
                existing = [
                    doc_id for doc_id, _ in self._catalog.list_namespace_documents(namespace)
                ]
                deletions: list[str] = []
                for doc_id in existing:
                    in_requested = (
                        requested == "."
                        or doc_id == requested
                        or doc_id.startswith(requested + "/")
                    )
                    explicit_nonmember = any(
                        doc_id == prefix or doc_id.startswith(prefix + "/") for prefix in nonmembers
                    )
                    if explicit_nonmember or (complete and in_requested and doc_id not in seen):
                        deletions.append(doc_id)
                for doc_id in sorted(set(deletions), key=str.encode):
                    try:
                        ready = self._publish_delete(DocumentId(namespace, doc_id))
                        removed.append(DocumentId(namespace, doc_id))
                        if not ready:
                            complete = False
                            stop = True
                            break
                    except (IndexFailed, IndexUnavailable, StorageFailed) as error:
                        failure(doc_id, error)
                        complete = False
                        stop = True
                        break

            return SyncReport(
                namespace=namespace,
                path=requested,
                complete=complete and not stop,
                changed=tuple(sorted(set(changed), key=_sort_id)),
                removed=tuple(sorted(set(removed), key=_sort_id)),
                failed=tuple(
                    sorted(
                        failed.values(), key=lambda item: (item.path.encode(), item.code.encode())
                    )
                ),
                skipped=tuple(
                    sorted(
                        skipped.values(),
                        key=lambda item: (item.path.encode(), item.reason.encode()),
                    )
                ),
                index_ready=self._state == "ready",
            )

    def _stat_unchanged(
        self, relative: str, source_stat: os.stat_result, previous: dict[str, Any] | None
    ) -> bool:
        if previous is None:
            return False
        suffix = suffix_for(relative)
        match: tuple[str, Processor] | None = None
        for processor in self._processors:
            media_type = self._processor_suffixes[id(processor)].get(suffix)
            if media_type is not None:
                match = (media_type, processor)
                break
        if match is None:
            return False
        media_type, processor = match
        source = previous.get("source", {})
        return (
            source.get("size") == source_stat.st_size
            and source.get("mtime_ns") == source_stat.st_mtime_ns
            and previous.get("media_type") == media_type
            and previous.get("processor") == self._processor_descriptions[id(processor)]
        )

    def _observe_target(self, root: Path, requested: str) -> tuple[Path | None, str]:
        if requested == ".":
            try:
                root_stat = root.stat()
            except OSError as error:
                raise SourceUnavailable(
                    f"external root is unavailable: {error}", path="."
                ) from error
            if not stat.S_ISDIR(root_stat.st_mode) or not os.access(root, os.R_OK):
                raise SourceUnavailable("external root is not a readable directory", path=".")
            return root, "."
        current = root
        actual: list[str] = []
        parts = requested.split("/")
        for index, part in enumerate(parts):
            try:
                entries = list(os.scandir(current))
            except OSError as error:
                raise SourceUnavailable(
                    f"cannot observe parent: {error}", path="/".join(actual) or "."
                ) from error
            entry = next((candidate for candidate in entries if candidate.name == part), None)
            if entry is None:
                requested_candidate = current / part
                for candidate in entries:
                    if candidate.name.casefold() != part.casefold():
                        continue
                    try:
                        if os.path.samefile(candidate.path, requested_candidate):
                            entry = candidate
                            break
                    except OSError:
                        continue
            if entry is None:
                return None, requested
            actual.append(entry.name)
            candidate_path = Path(entry.path)
            if index < len(parts) - 1:
                try:
                    candidate_stat = entry.stat(follow_symlinks=False)
                except OSError as error:
                    raise SourceUnavailable(str(error), path="/".join(actual)) from error
                if stat.S_ISLNK(candidate_stat.st_mode):
                    raise InvalidPath("sync path traverses a symlink", path="/".join(actual))
                if not stat.S_ISDIR(candidate_stat.st_mode):
                    return None, requested
            current = candidate_path
        return current, "/".join(actual)

    def _excluded(self, relative: str) -> bool:
        return any(_glob_match(pattern, relative) for pattern in self._sync_policy.exclude_globs)


def _paths_overlap(left: Path, right: Path) -> bool:
    try:
        left.relative_to(right)
        return True
    except ValueError:
        pass
    try:
        right.relative_to(left)
        return True
    except ValueError:
        return False


def _as_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CorruptState(f"{field} is not an integer")
    return value


def _runtime(value: object) -> object:
    return value


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


def _glob_match(pattern: str, relative: str) -> bool:
    import re

    expression: list[str] = ["^"]
    index = 0
    while index < len(pattern):
        character = pattern[index]
        if character == "*":
            if index + 1 < len(pattern) and pattern[index + 1] == "*":
                index += 2
                if index < len(pattern) and pattern[index] == "/":
                    expression.append("(?:.*/)?")
                    index += 1
                else:
                    expression.append(".*")
                continue
            expression.append("[^/]*")
        elif character == "?":
            expression.append("[^/]")
        elif character == "[":
            end = pattern.find("]", index + 1)
            if end < 0:
                expression.append(r"\[")
            else:
                content = pattern[index + 1 : end]
                if content.startswith("!"):
                    content = "^" + content[1:]
                expression.append("[" + content.replace("\\", r"\\") + "]")
                index = end
        else:
            expression.append(re.escape(character))
        index += 1
    expression.append("$")
    return re.fullmatch("".join(expression), relative) is not None
