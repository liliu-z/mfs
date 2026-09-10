# pyright: reportPrivateUsage=false
from __future__ import annotations

import contextlib
import copy
import math
import os
import shutil
import stat
import threading
import time
import uuid
from collections.abc import Generator, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, cast

import blake3
from filelock import FileLock, Timeout

from ._artifacts import ArtifactHandle, ArtifactStore
from ._catalog import Catalog
from ._filters import compile_filters
from ._index import ChunkIndex, IndexRow, SearchHit, dense_config, index_config
from ._json import JSONValue, canonical_json, compact_json, copy_json, load_json
from ._locks import Lifecycle
from ._platform import descriptor_change_time
from ._regex import regex_ranges
from ._validation import (
    chunker_description,
    normalized_media_type,
    processor_description,
    suffix_for,
    validate_chunk_ranges,
    validate_chunker,
    validate_embedder,
    validate_internal_id,
    validate_namespace,
    validate_processors,
)
from .adapters import DefaultChunker
from .errors import (
    CapabilityUnavailable,
    Closed,
    CorruptState,
    EmbeddingFailed,
    IdempotencyConflict,
    IndexFailed,
    IndexUnavailable,
    InstanceLocked,
    InvalidConfiguration,
    InvalidFilter,
    InvalidPattern,
    InvalidQuery,
    MFSError,
    NamespaceConflict,
    NamespaceNotFound,
    OperationFailed,
    ProcessingFailed,
    RetryableError,
    RootOverlap,
    SchemaVersionUnsupported,
    SourceChanged,
    SourceUnavailable,
    StorageFailed,
    Superseded,
    UnsupportedMediaType,
    WaitTimeout,
    WrongNamespaceKind,
)
from .processing import Cancellation, _ProcessingStopped, _ProcessingYielded
from .types import (
    Chunk,
    Chunker,
    Consistency,
    Document,
    DocumentId,
    DocumentStatus,
    DropReport,
    Embedder,
    Filter,
    GCPolicy,
    GCReport,
    IndexState,
    Match,
    MutationReport,
    NamespaceInfo,
    NamespaceKind,
    PreparationPolicy,
    Processor,
    Progress,
    QueryItem,
    QueryResult,
    ReindexReport,
    ScopeStatus,
    SearchItem,
    SearchMode,
    SearchResult,
    Select,
    SourceLocation,
    SourceMap,
    SourceSpan,
    Status,
    SyncPolicy,
    SyncReport,
    TaskError,
    TaskStage,
    TaskState,
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
        *,
        preparation_policy: PreparationPolicy | None = None,
        gc_policy: GCPolicy | None = None,
    ) -> MFS:
        return cls(
            mfs_path, processors, chunker, embedder, sync_policy, preparation_policy, gc_policy
        )

    def __init__(
        self,
        mfs_path: Path,
        processors: Sequence[Processor],
        chunker: Chunker | None,
        embedder: Embedder | None,
        sync_policy: SyncPolicy | None,
        preparation_policy: PreparationPolicy | None,
        gc_policy: GCPolicy | None,
    ) -> None:
        self._path = Path(mfs_path).expanduser().resolve()
        self._processors = validate_processors(processors)
        self._chunker = validate_chunker(chunker or DefaultChunker())
        self._embedder = validate_embedder(embedder)
        self._processor_descriptions = {id(p): processor_description(p) for p in self._processors}
        self._processor_media_types = {id(p): tuple(p.media_types) for p in self._processors}
        self._processor_suffixes = {id(p): dict(p.suffix_media_types) for p in self._processors}
        self._chunker_description = chunker_description(self._chunker)
        self._embedder_space = self._embedder.embedding_space if self._embedder else None
        self._embedder_dimension = self._embedder.dimension if self._embedder else None
        self._sync_policy = self._validate_sync_policy(sync_policy or SyncPolicy())
        self._condition = threading.Condition(threading.RLock())
        self._mutation_lock = threading.RLock()
        self._preparation_policy = preparation_policy or PreparationPolicy()
        self._gc_policy = gc_policy or GCPolicy()
        for policy in (self._preparation_policy, self._gc_policy):
            for key, value in asdict(policy).items():
                if key != "enabled" and (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    or value < 0
                ):
                    raise InvalidConfiguration(f"invalid policy value: {key}")
        for count in (
            self._preparation_policy.light_workers,
            self._preparation_policy.heavy_workers,
            self._gc_policy.batch_files,
            self._gc_policy.cycle_files,
        ):
            if not isinstance(_runtime(count), int) or isinstance(_runtime(count), bool):
                raise InvalidConfiguration("worker counts and file budgets must be integers")
        if (
            min(
                self._preparation_policy.light_workers,
                self._preparation_policy.heavy_workers,
                self._gc_policy.batch_files,
                self._gc_policy.cycle_files,
            )
            < 1
            or self._gc_policy.interval <= 0
            or self._preparation_policy.aging_seconds <= 0
        ):
            raise InvalidConfiguration("worker counts, file budgets and intervals must be positive")
        self._artifact_readers = 0
        self._last_activity = time.monotonic()
        self._active_scopes: tuple[UnderPath, ...] = ()
        self._progress: dict[DocumentId, dict[str, Any]] = {}
        self._cancellations: dict[tuple[DocumentId, str], Cancellation] = {}
        self._preparing: dict[tuple[DocumentId, str], int] = {}
        self._processing_keys: dict[tuple[DocumentId, str], str] = {}
        self._processor_resources: dict[int, tuple[str, int]] = {}
        for processor in self._processors:
            workload = getattr(processor, "workload", "light")
            concurrency = getattr(processor, "concurrency", 1)
            if (
                workload not in ("light", "heavy")
                or isinstance(concurrency, bool)
                or not isinstance(concurrency, int)
                or concurrency < 1
            ):
                raise InvalidConfiguration(
                    "processor workload must be light/heavy and concurrency a positive integer"
                )
            self._processor_resources[id(processor)] = (workload, concurrency)
        self._chunker_lock = threading.Lock()
        self._lifecycle = Lifecycle()
        self._stopping = False
        self._state: IndexState = "dirty"
        self._targets: dict[DocumentId, dict[str, Any]] = {}
        self._pending: dict[DocumentId, str] = {}
        self._namespaces: dict[str, dict[str, Any]] = {}
        self._workers: list[threading.Thread] = []
        self._executing: set[tuple[DocumentId, str]] = set()
        try:
            self._path.mkdir(parents=True, exist_ok=True)
            existing = list(self._path.iterdir())
            initialize = not existing
            if existing and not (self._path / "catalog.sqlite").is_file():
                raise CorruptState("non-empty mfs_path has no recognizable catalog")
            self._instance_lock = FileLock(self._path / "LOCK", thread_local=False)
            try:
                self._instance_lock.acquire(timeout=0)
            except Timeout as error:
                raise InstanceLocked(f"MFS instance is already open: {self._path}") from error
            for name in ("objects", "artifacts", "staging", "work"):
                (self._path / name).mkdir(exist_ok=True)
            self._catalog = Catalog(self._path / "catalog.sqlite", initialize=initialize)
            self._artifacts = ArtifactStore(self, self._gc_policy)
            with self._catalog.transaction():
                for name, record in self._catalog.list_namespaces():
                    record.setdefault("incarnation", uuid.uuid4().hex)
                    record.setdefault("binding", uuid.uuid4().hex)
                    record.setdefault("root_actual", record.get("root"))
                    self._catalog.put_namespace(name, record)
                    self._namespaces[name] = record
            self._index = ChunkIndex(self._path / "milvus.db")
            desired = index_config(
                cast(dict[str, object], self._chunker_description), self._provided_dense()
            )
            self._config: dict[str, object] = desired
            if not initialize:
                try:
                    loaded = load_json((self._path / "index.json").read_text())
                    if not isinstance(loaded, dict):
                        raise ValueError("index config is not an object")
                    if _as_int(loaded.get("version", 0), "index version") > 2:
                        raise SchemaVersionUnsupported("unsupported index schema")
                    self._config = cast(dict[str, object], loaded)
                except SchemaVersionUnsupported:
                    raise
                except (OSError, ValueError):
                    self._config = desired
            dense = self._dense_config()
            dimension = _as_int(dense["dimension"], "dense dimension") if dense else None
            invalid = not self._index.has_valid_collection(dense_dimension=dimension)
            # Migration and interrupted full rebuilds start from authoritative SQLite snapshots.
            rebuild = initialize or invalid or (self._path / "INDEX_DIRTY").exists()
            if rebuild:
                self._index.recreate(dense_dimension=dimension)
                self._config["version"] = 2
                self._write_index_config(self._config)
            else:
                self._index.load()
            with self._catalog.transaction():
                for ns, doc, job in self._catalog.list_targets():
                    if job["state"] in ("running", "blocked"):
                        job["state"] = "pending"
                        job["next_run"] = 0
                        self._catalog.put_target(ns, doc, job)
                    self._targets[DocumentId(ns, doc)] = job
                for ns, doc, record in self._catalog.list_documents():
                    identity = DocumentId(ns, doc)
                    if identity not in self._targets:
                        job = self._snapshot_job(identity, record)
                        self._catalog.put_target(ns, doc, job)
                        self._targets[identity] = job
                if rebuild:
                    for identity, original in list(self._targets.items()):
                        job = copy.deepcopy(original)
                        if (
                            job["kind"] == "upsert"
                            and job["stage"] != "process"
                            and job["state"] != "cancelled"
                        ):
                            job.update(stage="chunk", state="pending", vectors=[], next_run=0)
                            self._catalog.put_target(identity.namespace, identity.doc_id, job)
                            self._targets[identity] = job
                self._refresh_pending()
            mismatch = self._config.get("chunker") != self._chunker_description
            if self._embedder is not None:
                mismatch |= self._dense_config() != self._provided_dense()
            self._state = "mismatch" if mismatch else "ready"
            with contextlib.suppress(FileNotFoundError):
                (self._path / "INDEX_DIRTY").unlink()
            self._recover_objects()
            roles = [
                *("prepare-light" for _ in range(self._preparation_policy.light_workers)),
                *("prepare-heavy" for _ in range(self._preparation_policy.heavy_workers)),
                "index",
            ]
            for role in roles:
                thread = threading.Thread(target=self._worker, args=(role,), name=f"mfs-{role}")
                thread.start()
                self._workers.append(thread)
            if self._gc_policy.enabled:
                thread = threading.Thread(target=self._artifacts.maintain, name="mfs-maintenance")
                thread.start()
                self._workers.append(thread)
        except Exception:
            self._stopping = True
            with self._condition:
                for cancellation in self._cancellations.values():
                    cancellation._cancel("close")
                self._condition.notify_all()
            for thread in self._workers:
                thread.join()
            for name in ("_index", "_catalog"):
                with contextlib.suppress(Exception):
                    getattr(self, name).close()
            with contextlib.suppress(Exception):
                self._instance_lock.release()
            raise

    def _provided_dense(self) -> dict[str, object] | None:
        if self._embedder_space is None or self._embedder_dimension is None:
            return None
        return dense_config(self._embedder_space, self._embedder_dimension)

    @staticmethod
    def _validate_sync_policy(policy: SyncPolicy) -> SyncPolicy:
        maximum = policy.max_file_bytes
        if maximum is not None and (
            isinstance(_runtime(maximum), bool)
            or not isinstance(_runtime(maximum), int)
            or maximum < 0
        ):
            raise InvalidConfiguration("max_file_bytes must be non-negative or None")
        for pattern in policy.exclude_globs:
            if (
                not isinstance(_runtime(pattern), str)
                or not pattern
                or "\0" in pattern
                or "\\" in pattern
            ):
                raise InvalidConfiguration("exclude_globs must be non-empty POSIX patterns")
            _glob_match(pattern, "")
        return SyncPolicy(tuple(policy.exclude_globs), maximum)

    def _dense_config(self) -> dict[str, object] | None:
        dense = self._config.get("dense")
        return cast(dict[str, object], dense) if isinstance(dense, dict) else None

    def _is_ready(self) -> bool:
        return self._state == "ready" and not self._pending

    def _refresh_pending(self) -> None:
        self._pending = {
            identity: str(job["revision"])
            for identity, job in self._targets.items()
            if job["state"] != "succeeded"
        }

    def _remember(self, identity: DocumentId, job: dict[str, Any]) -> None:
        if self._targets.get(identity, {}).get("revision") != job["revision"]:
            self._progress.pop(identity, None)
        for (running_id, token), cancellation in self._cancellations.items():
            if running_id == identity and (
                job.get("attempt_token") != token or job["state"] == "cancelled"
            ):
                cancellation._cancel("user" if job["state"] == "cancelled" else "superseded")
        self._targets[identity] = copy.deepcopy(job)
        if job["state"] == "succeeded":
            self._pending.pop(identity, None)
        else:
            self._pending[identity] = str(job["revision"])
        self._condition.notify_all()

    def _store_job(self, identity: DocumentId, job: dict[str, Any]) -> None:
        with self._catalog.transaction():
            self._catalog.put_target(identity.namespace, identity.doc_id, job)
        self._remember(identity, job)

    def _current(self, identity: DocumentId, job: dict[str, Any]) -> bool:
        current = self._targets.get(identity)
        return (
            current is not None
            and current["revision"] == job["revision"]
            and current["state"] != "cancelled"
            and current.get("attempt_token") == job.get("attempt_token")
        )

    def _snapshot_job(self, identity: DocumentId, record: dict[str, Any]) -> dict[str, Any]:
        revision = uuid.uuid4().hex
        record = dict(record, revision=revision)
        self._catalog.put_document(identity.namespace, identity.doc_id, record)
        artifact = self._write_artifact(revision + "-snapshot", record)
        return dict(
            revision=revision,
            kind="upsert",
            stage="chunk",
            state="pending",
            attempts=0,
            failures=0,
            next_run=0,
            error=None,
            snapshot=artifact,
            vectors=[],
            incarnation=self._namespaces[identity.namespace]["incarnation"],
            source_revision=record.get("revision"),
            input=record["source"].get("object"),
            content_hash=record["content_hash"],
            media_type=record["media_type"],
            processor=record["processor"],
            source=record["source"],
            binding=record.get("binding"),
        )

    @contextlib.contextmanager
    def _call(self, *, activity: bool = True) -> Generator[None]:
        with self._lifecycle.call():
            with self._condition:
                if activity:
                    self._artifact_readers += 1
                    self._last_activity = time.monotonic()
            try:
                yield
            finally:
                with self._condition:
                    if activity:
                        self._artifact_readers -= 1
                        self._last_activity = time.monotonic()

    def close(self) -> None:
        with self._condition:
            self._stopping = True
            for cancellation in self._cancellations.values():
                cancellation._cancel("close")
            self._condition.notify_all()
        if not self._lifecycle.begin_close():
            return
        try:
            for thread in self._workers:
                thread.join()
            try:
                self._index.close()
            finally:
                self._catalog.close()
                self._instance_lock.release()
        finally:
            self._lifecycle.finish_close()

    def wait(
        self, receipt: MutationReport | DropReport | SyncReport, timeout: float | None = None
    ) -> None:
        """Wait only for the durable, sealed targets belonging to this receipt."""
        self._validate_timeout(timeout)
        with self._call(activity=False), self._condition:
            operation_id = receipt.operation_id
            if operation_id is None:
                if isinstance(receipt, MutationReport) and receipt.revision is not None:
                    revisions = [receipt.revision]  # Receipts issued before schema 3.
                elif isinstance(receipt, SyncReport) and not receipt.complete:
                    raise OperationFailed("sync observation was incomplete", state="incomplete")
                else:
                    return
            else:
                row = self._catalog.connection.execute(
                    "SELECT targets,complete FROM wait_operations WHERE operation_id=?",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    raise InvalidQuery("receipt does not belong to this MFS catalog")
                if not row[1]:
                    raise OperationFailed("sync observation was incomplete", state="incomplete")
                revisions = cast(list[str], load_json(row[0]))
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                if self._stopping:
                    raise Closed("MFS instance is closing")
                pending = False
                targets = list(revisions)
                visited: set[str] = set()
                while targets:
                    revision = targets.pop()
                    if revision in visited:
                        continue
                    visited.add(revision)
                    targets.extend(
                        str(row[0])
                        for row in self._catalog.connection.execute(
                            "SELECT child FROM run_dependencies WHERE parent=?", (revision,)
                        )
                    )
                    row = self._catalog.connection.execute(
                        "SELECT state,error,error_code,retryable FROM runs WHERE revision=?",
                        (revision,),
                    ).fetchone()
                    if row is None:
                        raise Superseded(
                            "target history is unavailable", revision=revision, state="superseded"
                        )
                    state, error, code, retryable = row
                    if state == "superseded":
                        raise Superseded(
                            "target was superseded before completion",
                            revision=revision,
                            state=state,
                        )
                    if state in ("cancelled", "failed", "blocked"):
                        raise OperationFailed(
                            error or f"target is {state}",
                            revision=revision,
                            state=state,
                            error_code=code,
                            retryable=bool(retryable),
                        )
                    pending |= state != "succeeded"
                if not pending:
                    return
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise WaitTimeout("operation targets have not completed")
                self._condition.wait(remaining)

    def index_configuration(self) -> JSONValue:
        with self._call(activity=False), self._condition:
            return copy_json(cast(JSONValue, self._config))

    def scope_status(self, namespace: str | None = None, path: str = ".") -> ScopeStatus:
        with self._call(activity=False), self._condition:
            states: dict[str, int] = {}
            stages: dict[str, int] = {}
            for identity, job in self._targets.items():
                if (
                    not identity.namespace
                    or (namespace is not None and namespace != identity.namespace)
                    or not self._under(identity.doc_id, path)
                ):
                    continue
                states[job["state"]] = states.get(job["state"], 0) + 1
                stages[job["stage"]] = stages.get(job["stage"], 0) + 1
            return ScopeStatus(sum(states.values()), states, stages)

    @staticmethod
    def _under(doc_id: str, path: str) -> bool:
        return path == "." or doc_id == path or doc_id.startswith(path + "/")

    def set_active_scopes(self, scopes: Sequence[UnderPath]) -> None:
        with self._call(activity=False), self._condition:
            for scope in scopes:
                self._required_namespace(scope.namespace)
            self._active_scopes = tuple(scopes)
            self._condition.notify_all()

    def _priority(self, identity: DocumentId, job: dict[str, Any]) -> tuple[float, float, str]:
        if job["kind"] in ("drop", "delete", "rebuild"):
            return (-100.0, 0.0, identity.doc_id)
        base = (
            0
            if job.get("force")
            else 1
            if any(
                scope.namespace == identity.namespace and self._under(identity.doc_id, scope.path)
                for scope in self._active_scopes
            )
            else 2
        )
        enqueued = float(job.get("enqueued_at", 0))
        age = max(0, time.time() - enqueued) / self._preparation_policy.aging_seconds
        return (base - age, enqueued, identity.doc_id)

    def _processor_for(self, job: dict[str, Any]) -> Processor | None:
        return next(
            (
                p
                for p in self._processors
                if self._processor_descriptions[id(p)] == job.get("processor")
            ),
            None,
        )

    def open_artifact(self, document_id: DocumentId, name: str) -> ArtifactHandle:
        lease = self._call()
        lease.__enter__()
        try:
            with self._condition:
                row = self._catalog.connection.execute(
                    "SELECT json_extract(value,'$.snapshot_id'),json_extract(value,'$.artifacts') "
                    "FROM documents WHERE namespace=? AND doc_id=?",
                    (document_id.namespace, document_id.doc_id),
                ).fetchone()
                artifacts = cast(dict[str, str], load_json(row[1])) if row and row[1] else {}
                if name not in artifacts:
                    raise InvalidQuery("document has no artifact with this name")
                path = self._path / artifacts[name]
                if path.parent != self._path / "artifacts" or path.is_symlink():
                    raise CorruptState("invalid artifact path")
            return ArtifactHandle(
                path.open("rb"), str(row[0]), lambda: lease.__exit__(None, None, None)
            )
        except BaseException:
            lease.__exit__(None, None, None)
            raise

    def collect_garbage(self) -> GCReport:
        # Lifecycle pin only: this call must not count as foreground artifact use.
        with self._lifecycle.call():
            return self._artifacts.collect()

    def garbage_collection_status(self) -> GCReport:
        with self._lifecycle.call():
            return self._artifacts.last_report

    def wait_ready(self, timeout: float | None = None) -> None:
        with self._call(activity=False):
            self._wait_ready(timeout)

    def _wait_ready(self, timeout: float | None) -> None:
        self._validate_timeout(timeout)
        with self._condition:
            if self._state in ("dirty", "mismatch"):
                raise IndexUnavailable(f"index state is {self._state}; call reindex()")
            completed = self._condition.wait_for(
                lambda: self._stopping or self._state != "ready" or not self._pending, timeout
            )
            if self._stopping:
                raise Closed("MFS instance is closing")
            if self._state != "ready":
                raise IndexUnavailable(f"index state is {self._state}")
            if not completed:
                raise WaitTimeout(f"{len(self._pending)} indexing targets have not completed")

    @staticmethod
    def _validate_timeout(timeout: float | None) -> None:
        if timeout is not None and (
            isinstance(_runtime(timeout), bool)
            or not isinstance(_runtime(timeout), (int, float))
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise InvalidQuery("timeout must be a finite non-negative number or None")

    def create_namespace(
        self, namespace: str, kind: NamespaceKind, root: Path | None = None
    ) -> NamespaceInfo:
        with self._call(), self._mutation_lock:
            validate_namespace(namespace)
            if kind not in ("internal", "external"):
                raise InvalidConfiguration("namespace kind must be internal or external")
            spelling: Path | None = None
            actual: Path | None = None
            if kind == "external":
                if root is None:
                    raise InvalidConfiguration("external namespace requires a root")
                spelling = Path(os.path.abspath(Path(root).expanduser()))
                try:
                    actual = spelling.resolve(strict=True)
                    if not actual.is_dir():
                        raise OSError("root is not a directory")
                except OSError as error:
                    raise SourceUnavailable(str(error)) from error
                if _paths_overlap(actual, self._path):
                    raise RootOverlap("external root and mfs_path must not overlap")
            elif root is not None:
                raise InvalidConfiguration("internal namespace must not have a root")
            with self._condition:
                previous = self._namespaces.get(namespace)
                if previous is not None:
                    if previous["kind"] == kind and previous["root"] == (
                        str(spelling) if spelling else None
                    ):
                        return self._namespace_info(namespace, previous)
                    raise NamespaceConflict(f"namespace {namespace!r} has another binding")
                record = dict(
                    version=2,
                    kind=kind,
                    root=str(spelling) if spelling else None,
                    root_actual=str(actual) if actual else None,
                    incarnation=uuid.uuid4().hex,
                    binding=uuid.uuid4().hex,
                )
                with self._catalog.transaction():
                    self._catalog.put_namespace(namespace, record)
                self._namespaces[namespace] = record
                return self._namespace_info(namespace, record)

    @staticmethod
    def _namespace_info(namespace: str, record: dict[str, Any]) -> NamespaceInfo:
        return NamespaceInfo(
            namespace,
            cast(NamespaceKind, record["kind"]),
            Path(record["root"]) if record["root"] is not None else None,
        )

    def _required_namespace(self, namespace: str) -> NamespaceInfo:
        validate_namespace(namespace)
        record = self._namespaces.get(namespace)
        if record is None:
            raise NamespaceNotFound(f"namespace {namespace!r} does not exist")
        return self._namespace_info(namespace, record)

    def get_namespace(self, namespace: str) -> NamespaceInfo:
        with self._call(), self._condition:
            return self._required_namespace(namespace)

    def list_namespaces(self) -> tuple[NamespaceInfo, ...]:
        with self._call(), self._condition:
            return tuple(
                self._namespace_info(n, self._namespaces[n])
                for n in sorted(self._namespaces, key=str.encode)
            )

    def drop_namespace(self, namespace: str) -> DropReport:
        with self._call(), self._mutation_lock, self._condition:
            validate_namespace(namespace)
            if namespace not in self._namespaces:
                job = self._targets.get(DocumentId(namespace, ""))
                operation_id = uuid.uuid4().hex
                with self._catalog.transaction():
                    self._catalog.add_wait_operation(
                        operation_id, [str(job["revision"])] if job else []
                    )
                return DropReport(
                    namespace, False, not self._pending and self._state == "ready", operation_id
                )
            # One durable namespace cleanup survives an immediate same-name recreation.
            identity = DocumentId(namespace, "")
            job = self._delete_job("drop")
            previous_drop = self._targets.get(identity, {})
            job["published_artifacts"] = {
                str(index): path
                for index, path in enumerate(
                    p
                    for i, j in self._targets.items()
                    if i.namespace == namespace
                    for p in j.get("published_artifacts", {}).values()
                )
            }
            job["incarnations"] = list(
                dict.fromkeys(
                    [
                        *previous_drop.get("incarnations", []),
                        self._namespaces[namespace]["incarnation"],
                    ]
                )
            )
            operation_id = uuid.uuid4().hex
            for (identity_running, _), cancellation in self._cancellations.items():
                if identity_running.namespace == namespace:
                    cancellation._cancel("drop")
            with self._catalog.transaction():
                self._catalog.delete_namespace(namespace)
                self._catalog.delete_targets(namespace)
                self._catalog.put_target(namespace, "", job)
                self._catalog.add_wait_operation(operation_id, [str(job["revision"])])
            self._namespaces.pop(namespace)
            self._targets = {i: j for i, j in self._targets.items() if i.namespace != namespace}
            self._refresh_pending()
            self._remember(identity, job)
            return DropReport(namespace, True, False, operation_id)

    def status(self) -> Status:
        with self._call(activity=False), self._condition:
            ready = self._state == "ready" and not self._pending
            return Status(
                self._catalog.namespace_count(),
                self._catalog.document_count(),
                self._state if self._state != "ready" or ready else "pending",
                self._dense_config() is not None,
                self._embedder is not None and self._dense_config() == self._provided_dense(),
                ready,
                len(self._pending),
                sum(j["state"] in ("failed", "blocked") for j in self._targets.values()),
            )

    def document_status(self, document_id: DocumentId) -> DocumentStatus | None:
        with self._call(activity=False), self._condition:
            job = self._targets.get(document_id)
            if job is None:
                return None
            text_revision = self._catalog.get_document_revision(
                document_id.namespace, document_id.doc_id
            )
            progress = self._progress.get(document_id, job.get("progress"))
            return DocumentStatus(
                document_id,
                str(job["revision"]),
                text_revision,
                job.get("indexed_revision"),
                cast(TaskStage, job["stage"]),
                cast(TaskState, job["state"]),
                int(job["attempts"]),
                job.get("error"),
                job.get("next_run") or None,
                len(job.get("vectors", [])),
                int(job.get("batches", 0)),
                any(identity == document_id for identity, _ in self._executing),
                job.get("content_hash"),
                job.get("media_type"),
                job.get("source", {}).get("size"),
                job.get("source", {}).get("mtime_ns"),
                TaskError(
                    job.get("error_code", "TaskFailed"), job["error"], bool(job.get("retryable"))
                )
                if job.get("error")
                else None,
                Progress(float(progress["completed"]), progress.get("total"), progress.get("unit"))
                if progress
                else None,
                tuple(job.get("artifacts", {})),
            )

    def list_document_statuses(
        self, namespace: str | None = None, *, path: str = ".", limit: int = 100, offset: int = 0
    ) -> tuple[DocumentStatus, ...]:
        with self._call(activity=False), self._condition:
            if (
                isinstance(_runtime(limit), bool)
                or not isinstance(_runtime(limit), int)
                or not 1 <= limit <= 1000
                or isinstance(_runtime(offset), bool)
                or not isinstance(_runtime(offset), int)
                or offset < 0
            ):
                raise InvalidQuery("limit must be 1..1000 and offset a non-negative integer")
            ids = sorted(
                (
                    i
                    for i in self._targets
                    if i.namespace
                    and (namespace is None or i.namespace == namespace)
                    and self._under(i.doc_id, path)
                ),
                key=_sort_id,
            )
            return tuple(
                s
                for i in ids[offset : offset + limit]
                if (s := self.document_status(i)) is not None
            )

    def upsert(
        self,
        namespace: str,
        doc_id: str,
        data: Path | bytes,
        media_type: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> MutationReport:
        with self._call(), self._mutation_lock:
            validate_internal_id(doc_id)
            if self._required_namespace(namespace).kind != "internal":
                raise WrongNamespaceKind("upsert requires an internal namespace")
            staged = (
                self._stage_bytes(data) if isinstance(data, bytes) else self._stage_path(Path(data))
            )
            try:
                return self._admit(
                    DocumentId(namespace, doc_id),
                    staged,
                    media_type=media_type,
                    fallback=data if isinstance(data, Path) else None,
                    idempotency_key=idempotency_key,
                )
            finally:
                self._remove_staging(staged.directory)

    def _admit(
        self,
        identity: DocumentId,
        staged: _Staged,
        *,
        media_type: str | None = None,
        fallback: Path | None = None,
        idempotency_key: str | None = None,
        force: bool = False,
    ) -> MutationReport:
        media, processor = self._select_processor(
            identity.doc_id, staged.path, media_type, fallback
        )
        description = self._processor_descriptions[id(processor)]
        if idempotency_key is not None and (
            not isinstance(_runtime(idempotency_key), str)
            or not idempotency_key
            or len(idempotency_key.encode()) > 2048
        ):
            raise InvalidQuery("idempotency_key must be a non-empty string of at most 2048 bytes")
        with self._condition:
            if self._stopping:
                raise Closed("MFS is closing")
            ns = self._namespaces[identity.namespace]
            fingerprint = dict(
                content_hash=staged.content_hash,
                media_type=media,
                processor=description,
                binding=ns["binding"],
            )
            request_hash = blake3.blake3(
                canonical_json(
                    dict(
                        namespace=identity.namespace,
                        doc_id=identity.doc_id,
                        incarnation=ns["incarnation"],
                        **fingerprint,
                    )
                )
            ).hexdigest()
            if idempotency_key is not None:
                prior = self._catalog.get_operation(idempotency_key)
                if prior is not None:
                    if prior[0] != request_hash:
                        raise IdempotencyConflict(
                            "idempotency key was used for a different request"
                        )
                    r = prior[1]
                    return MutationReport(
                        identity, r["outcome"], r["index_ready"], r["revision"], r["operation_id"]
                    )
            previous = self._targets.get(identity)
            unchanged = (
                not force
                and previous is not None
                and previous["kind"] == "upsert"
                and all(previous.get(k) == v for k, v in fingerprint.items())
            )
            operation_id = uuid.uuid4().hex
            if unchanged:
                assert previous is not None
                report = MutationReport(
                    identity,
                    "unchanged",
                    not self._pending and self._state == "ready",
                    previous["revision"],
                    operation_id,
                )
                with self._catalog.transaction():
                    self._catalog.add_wait_operation(operation_id, [str(previous["revision"])])
                    if idempotency_key is not None:
                        self._catalog.put_operation(idempotency_key, request_hash, asdict(report))
                return report
            revision = uuid.uuid4().hex
            object_name = "objects/" + revision
            os.replace(staged.path, self._path / object_name)
            self._fsync_directory(self._path / "objects")
            external = ns["kind"] == "external"
            source = dict(
                size=staged.size,
                mtime_ns=staged.mtime_ns if external else None,
                object=None if external else object_name,
            )
            # Preserve descendants until this replacement file has usable text, then index.
            children = (
                {
                    i.doc_id: j["revision"]
                    for i, j in self._targets.items()
                    if i.namespace == identity.namespace
                    and i.doc_id.startswith(identity.doc_id + "/")
                    and j["kind"] == "upsert"
                }
                if external
                else {}
            )
            job = dict(
                revision=revision,
                identity=asdict(identity),
                force=force,
                enqueued_at=time.time(),
                published_artifacts=previous.get("published_artifacts", {}) if previous else {},
                kind="upsert",
                stage="process",
                state="pending",
                attempts=0,
                failures=0,
                next_run=0,
                error=None,
                input=object_name,
                source=source,
                incarnation=ns["incarnation"],
                children=children,
                indexed_revision=previous.get("indexed_revision") if previous else None,
                vectors=[],
                **fingerprint,
            )
            if not force and self._catalog.cancelled(identity.namespace, identity.doc_id):
                job["state"] = "cancelled"
            existed = previous is not None and previous["kind"] == "upsert"
            report = MutationReport(
                identity, "updated" if existed else "added", False, revision, operation_id
            )
            try:
                with self._catalog.transaction():
                    if force:
                        self._catalog.set_cancelled(identity.namespace, identity.doc_id, False)
                    self._catalog.put_target(identity.namespace, identity.doc_id, job)
                    self._catalog.add_wait_operation(operation_id, [revision])
                    if idempotency_key is not None:
                        self._catalog.put_operation(idempotency_key, request_hash, asdict(report))
            except Exception:
                # A lost ACK must not leave durable accepted work out of the live pending set.
                durable = self._catalog.get_target(identity.namespace, identity.doc_id)
                if durable is not None:
                    self._remember(identity, durable)
                raise
            self._remember(identity, job)
            return report

    @staticmethod
    def _delete_job(kind: str = "delete") -> dict[str, Any]:
        return dict(
            revision=uuid.uuid4().hex,
            kind=kind,
            stage=kind,
            state="pending",
            attempts=0,
            failures=0,
            next_run=0,
            error=None,
        )

    def _remove(self, identity: DocumentId) -> MutationReport:
        with self._condition:
            old = self._targets.get(identity)
            if old is None or old["kind"] != "upsert":
                operation_id = uuid.uuid4().hex
                with self._catalog.transaction():
                    self._catalog.add_wait_operation(
                        operation_id, [str(old["revision"])] if old else []
                    )
                return MutationReport(
                    identity,
                    "not_found",
                    not self._pending and self._state == "ready",
                    old["revision"] if old else None,
                    operation_id,
                )
            job = self._delete_job()
            job["incarnation"] = old.get("incarnation")
            job["indexed_revision"] = old.get("indexed_revision")
            job["published_artifacts"] = old.get("published_artifacts", {})
            operation_id = uuid.uuid4().hex
            with self._catalog.transaction():
                self._catalog.delete_document(identity.namespace, identity.doc_id)
                self._catalog.put_target(identity.namespace, identity.doc_id, job)
                self._catalog.add_wait_operation(operation_id, [str(job["revision"])])
            self._remember(identity, job)
            return MutationReport(identity, "removed", False, job["revision"], operation_id)

    def remove(self, namespace: str, doc_id: str) -> MutationReport:
        with self._call(), self._mutation_lock:
            validate_internal_id(doc_id)
            if self._required_namespace(namespace).kind != "internal":
                raise WrongNamespaceKind(
                    "remove requires an internal namespace; use sync for external files"
                )
            return self._remove(DocumentId(namespace, doc_id))

    def retry(self, document_id: DocumentId, stage: TaskStage | None = None) -> None:
        with self._call(), self._condition:
            previous = self._targets.get(document_id)
            if previous is None:
                raise InvalidQuery("document has no task")
            if previous["state"] == "running":
                raise InvalidQuery("task is still executing")
            job = copy.deepcopy(previous)
            if stage is not None and stage != job["stage"]:
                raise InvalidQuery(
                    "retry resumes the failed stage; use reprocess for new processing"
                )
            if job["state"] == "succeeded":
                return
            job.update(
                state="pending", next_run=0, failures=0, error=None, attempt_token=uuid.uuid4().hex
            )
            with self._catalog.transaction():
                self._catalog.set_cancelled(document_id.namespace, document_id.doc_id, False)
                self._store_job(document_id, job)

    def cancel(self, document_id: DocumentId) -> None:
        with self._call(), self._condition:
            previous = self._targets.get(document_id)
            if previous is None:
                raise InvalidQuery("document has no task")
            if previous["state"] == "succeeded":
                return
            job = copy.deepcopy(previous)
            job["state"] = "cancelled"
            job["attempt_token"] = uuid.uuid4().hex
            with self._catalog.transaction():
                self._catalog.set_cancelled(document_id.namespace, document_id.doc_id, True)
                self._store_job(document_id, job)

    def reprocess(self, document_id: DocumentId) -> MutationReport:
        with self._call(), self._mutation_lock:
            info = self._required_namespace(document_id.namespace)
            if info.kind == "external":
                report = self._sync(
                    document_id.namespace, document_id.doc_id, verify="content", force=True
                )
                if report.failed or report.skipped or not report.changed:
                    raise SourceUnavailable("external input could not be reprocessed")
                job = self._targets[document_id]
                return MutationReport(
                    document_id, "updated", False, job["revision"], report.operation_id
                )
            with self._condition:
                job = self._targets.get(document_id)
                if job is None or job["kind"] != "upsert":
                    raise InvalidQuery("document does not exist")
                input_name = job.get("input") or job.get("source", {}).get("object")
            staged = self._stage_path(self._path / str(input_name))
            try:
                return self._admit(document_id, staged, media_type=job["media_type"], force=True)
            finally:
                self._remove_staging(staged.directory)

    def _worker(self, role: str) -> None:
        while True:
            with self._condition:
                if self._stopping:
                    return
                now = time.time()
                chosen: tuple[DocumentId, dict[str, Any]] | None = None
                next_run: float | None = None
                for identity, item in sorted(
                    (
                        (i, self._targets[i])
                        for i in self._pending
                        if self._targets[i]["state"] in ("pending", "retry_wait")
                    ),
                    key=lambda pair: self._priority(*pair),
                ):
                    if item["state"] not in ("pending", "retry_wait"):
                        continue
                    if (item["stage"] == "process") != role.startswith("prepare"):
                        continue
                    if any(i == identity for i, _ in self._executing):
                        continue
                    if role.startswith("prepare"):
                        processor = self._processor_for(item)
                        workload, concurrency = (
                            self._processor_resources[id(processor)]
                            if processor is not None
                            else ("light", 1)
                        )
                        if role != "prepare-" + workload:
                            continue
                        if (
                            processor is not None
                            and sum(p == id(processor) for p in self._preparing.values())
                            >= concurrency
                        ):
                            continue
                        if processor is not None:
                            from ._preparation import cache_key

                            if (
                                cache_key(self, identity, item, processor)
                                in self._processing_keys.values()
                            ):
                                continue
                    if (
                        role == "index"
                        and self._state != "ready"
                        and item["kind"] not in ("rebuild", "delete", "drop")
                    ):
                        continue
                    dependency = item.get("depends_on")
                    if dependency:
                        parent = self._targets.get(DocumentId(identity.namespace, dependency))
                        if parent is not None and parent["state"] != "succeeded":
                            continue
                    due = float(item.get("next_run", 0))
                    if due > now:
                        next_run = due if next_run is None else min(next_run, due)
                        continue
                    # Namespace cleanup runs before rows for a newly created incarnation.
                    if chosen is None or item["kind"] in ("delete", "drop", "rebuild"):
                        chosen = identity, copy.deepcopy(item)
                        if item["kind"] in ("delete", "drop", "rebuild"):
                            break
                if chosen is None:
                    self._condition.wait(None if next_run is None else max(0.01, next_run - now))
                    continue
                identity, job = chosen
                job.update(
                    state="running",
                    attempts=int(job.get("attempts", 0)) + 1,
                    attempt_token=uuid.uuid4().hex,
                )
                try:
                    self._store_job(identity, job)
                except Exception:
                    # A failed claim has not executed an external action. Retry durable state later.
                    self._condition.wait(0.25)
                    continue
                execution = (identity, str(job["attempt_token"]))
                self._executing.add(execution)
                self._last_activity = time.monotonic()
                self._cancellations[execution] = Cancellation()
                if role.startswith("prepare"):
                    processor = self._processor_for(job)
                    self._preparing[execution] = id(processor)
                    if processor is not None:
                        from ._preparation import cache_key

                        self._processing_keys[execution] = cache_key(self, identity, job, processor)
                job.setdefault("identity", asdict(identity))
            try:
                if role.startswith("prepare"):
                    self._process_job(identity, job)
                else:
                    self._index_job(identity, job)
            except _ProcessingYielded:
                self._advance(identity, job)
            except _ProcessingStopped:
                with self._condition:
                    if self._current(identity, job):
                        self._advance(identity, job)
            except Exception as error:
                self._fail_job(identity, job, error)
            finally:
                with self._condition:
                    self._executing.discard(execution)
                    self._preparing.pop(execution, None)
                    self._processing_keys.pop(execution, None)
                    self._cancellations.pop(execution, None)
                    self._last_activity = time.monotonic()
                    self._condition.notify_all()

    def _fail_job(self, identity: DocumentId, job: dict[str, Any], error: Exception) -> None:
        with self._condition:
            if not self._current(identity, job):
                return
            # Handlers may have changed their local stage before a transaction rolled back.
            # Resume the durable stage, or adopt a transaction that committed before raising.
            previous = self._targets[identity]
            try:
                durable = self._catalog.get_target(identity.namespace, identity.doc_id)
            except Exception:
                durable = None
            if durable is not None and durable != previous:
                self._remember(identity, durable)
                return
            job = copy.deepcopy(durable or previous)
            failures = int(job.get("failures", 0)) + 1
            cause: BaseException | None = error
            retryable = False
            visited: set[int] = set()
            while cause is not None and id(cause) not in visited:
                visited.add(id(cause))
                retryable |= isinstance(
                    cause, (RetryableError, TimeoutError, ConnectionError, OSError, StorageFailed)
                )
                cause = cause.__cause__
            state = (
                "blocked"
                if isinstance(error, CapabilityUnavailable)
                else "retry_wait"
                if retryable and failures < 5
                else "failed"
            )
            job.update(
                state=state,
                failures=failures,
                error=str(error),
                error_code=error.code if isinstance(error, MFSError) else type(error).__name__,
                retryable=retryable,
                next_run=time.time() + min(30, 0.25 * 2 ** (failures - 1))
                if state == "retry_wait"
                else 0,
            )
            try:
                self._store_job(identity, job)
            except Exception as persistence_error:
                # Keep failed work pending even when the completion/error transaction itself fails.
                job.update(
                    state="retry_wait",
                    next_run=time.time() + 0.5,
                    error=f"{error}; state persistence failed: {persistence_error}",
                )
                self._remember(identity, job)

    def _advance(self, identity: DocumentId, job: dict[str, Any], **changes: Any) -> None:
        with self._condition:
            if not self._current(identity, job):
                return
            job.update(state="pending", error=None, failures=0, next_run=0, **changes)
            self._store_job(identity, job)

    def _prepare_snapshot(self, job: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        saved = self._catalog.connection.execute(
            "SELECT path FROM prepared WHERE revision=?", (job["revision"],)
        ).fetchone()
        artifact = str(saved[0]) if saved else "artifacts/" + job["revision"] + "-snapshot.json"
        if (self._path / artifact).exists():
            cached: object = self._read_artifact(artifact)
            if not isinstance(cached, dict):
                raise CorruptState("processed artifact is not an object")
            snapshot = cast(dict[str, Any], cached)
            if snapshot.get("revision") != job["revision"]:
                raise CorruptState("processed artifact does not match target revision")
            return artifact, snapshot
        from ._preparation import prepare

        processor = self._processor_for(job)
        if processor is None:
            raise CapabilityUnavailable("registered Processor does not match accepted input")
        prepared = prepare(self, DocumentId(**job["identity"]), job, processor)
        record: dict[str, Any] = dict(
            version=2,
            revision=job["revision"],
            media_type=job["media_type"],
            content_hash=job["content_hash"],
            processor=job["processor"],
            **prepared,
            source=job["source"],
            binding=job["binding"],
            incarnation=job["incarnation"],
        )
        record["identity"] = job["identity"]
        record["preparation_attempt"] = job.get("attempt_token")
        record["snapshot_id"] = blake3.blake3(compact_json(record).encode()).hexdigest()
        artifact = self._write_artifact(job["revision"] + "-snapshot", record)
        return artifact, record

    def _process_job(self, identity: DocumentId, job: dict[str, Any]) -> None:
        artifact, record = self._prepare_snapshot(job)
        with self._condition:
            if not self._current(identity, job):
                return
            updates: list[tuple[DocumentId, dict[str, Any]]] = []
            with self._catalog.transaction():
                self._catalog.put_document(identity.namespace, identity.doc_id, record)
                for child, revision in job.get("children", {}).items():
                    child_id = DocumentId(identity.namespace, child)
                    previous = self._targets.get(child_id)
                    if previous is None or previous["revision"] != revision:
                        continue
                    deletion = self._delete_job()
                    deletion.update(
                        depends_on=identity.doc_id,
                        incarnation=previous.get("incarnation"),
                        indexed_revision=previous.get("indexed_revision"),
                        published_artifacts=previous.get("published_artifacts", {}),
                    )
                    self._catalog.delete_document(identity.namespace, child)
                    self._catalog.put_target(identity.namespace, child, deletion)
                    self._catalog.connection.execute(
                        "INSERT OR IGNORE INTO run_dependencies VALUES(?,?)",
                        (job["revision"], deletion["revision"]),
                    )
                    updates.append((child_id, deletion))
                job.update(
                    stage="chunk",
                    state="pending",
                    snapshot=artifact,
                    artifacts=record.get("artifacts", {}),
                    checkpoint={},
                    error=None,
                    failures=0,
                )
                self._catalog.put_target(identity.namespace, identity.doc_id, job)
            for child_id, deletion in updates:
                self._remember(child_id, deletion)
            self._remember(identity, job)

    def _index_job(self, identity: DocumentId, job: dict[str, Any]) -> None:
        if job["kind"] == "rebuild":
            self._rebuild_job(identity, job)
            return
        with self._condition:
            if not self._current(identity, job):
                return
        stage = job["stage"]
        if stage == "drop":
            for incarnation in job.get("incarnations", [None]):
                self._index.delete_namespace(identity.namespace, incarnation=incarnation)
        elif stage == "delete":
            self._index.delete_document(identity, incarnation=job.get("incarnation"))
            self._index.flush()
        elif stage == "chunk":
            record = self._read_artifact(job["snapshot"])
            text = str(record["text"])
            key = self._artifacts.key(
                "chunk",
                dict(text=text, source_map=record["source_map"], chunker=self._chunker_description),
            )
            chunks = self._artifacts.cached(key)
            if chunks is None:
                with self._chunker_lock:
                    ranges = validate_chunk_ranges(
                        text, self._chunker.chunk(text, self._source_map(record))
                    )
                encoded = text.encode()
                chunks = [
                    dict(
                        ordinal=i,
                        text_start=r.text_start,
                        text_end=r.text_end,
                        text=encoded[r.text_start : r.text_end].decode(),
                    )
                    for i, r in enumerate(ranges)
                ]
            artifact = self._write_artifact(job["revision"] + "-chunks", chunks)
            self._artifacts.cache(key, artifact)
            self._advance(
                identity,
                job,
                stage="embed" if self._dense_config() and chunks else "publish",
                chunks=artifact,
                vectors=[],
                batches=(len(chunks) + 127) // 128,
            )
            return
        elif stage == "embed":
            chunks = self._read_artifact(job["chunks"])
            batch = len(job.get("vectors", []))
            texts = [str(c["text"]) for c in chunks[batch * 128 : (batch + 1) * 128]]
            key = self._artifacts.key(
                "vectors", dict(texts=texts, dense=self._dense_config(), purpose="document")
            )
            vectors = self._artifacts.cached(key)
            if vectors is not None:
                try:
                    vectors = self._validate_vectors(
                        vectors,
                        len(texts),
                        _as_int((self._dense_config() or {})["dimension"], "dense dimension"),
                    )
                except Exception:
                    vectors = None
            if vectors is None:
                vectors = self._embed_documents(texts)
            artifact = self._write_artifact(job["revision"] + f"-vectors-{batch}", vectors)
            with self._condition:
                if not self._current(identity, job):
                    return
            self._artifacts.cache(key, artifact)
            paths = [*job.get("vectors", []), artifact]
            self._advance(
                identity,
                job,
                stage="publish" if len(paths) == job["batches"] else "embed",
                vectors=paths,
            )
            return
        elif stage == "publish":
            rows = self._job_rows(identity, job)
            with self._condition:
                if not self._current(identity, job):
                    return
            self._index.replace(identity, rows)
        else:
            raise CorruptState(f"unknown task stage {stage!r}")
        with self._condition:
            if self._current(identity, job):
                job.update(
                    state="succeeded",
                    indexed_revision=job["revision"] if job["kind"] == "upsert" else None,
                    published_artifacts=job.get("artifacts", {}),
                    error=None,
                    failures=0,
                )
                self._store_job(identity, job)

    def _job_rows(self, identity: DocumentId, job: dict[str, Any]) -> list[IndexRow]:
        record = self._read_artifact(job["snapshot"])
        chunks = self._read_artifact(job["chunks"])
        vectors = [
            vector for name in job.get("vectors", []) for vector in self._read_artifact(name)
        ]
        if self._dense_config() is not None and len(vectors) != len(chunks):
            raise CorruptState("publication is missing dense vectors")
        source_map = self._source_map(record)
        rows: list[IndexRow] = []
        for i, chunk in enumerate(chunks):
            location = self._source_location(source_map, chunk["text_start"], chunk["text_end"])
            rows.append(
                IndexRow(
                    namespace=identity.namespace,
                    doc_id=identity.doc_id,
                    ordinal=i,
                    text=chunk["text"],
                    text_start=chunk["text_start"],
                    text_end=chunk["text_end"],
                    dense_vector=vectors[i] if vectors else [],
                    snapshot_id=record["snapshot_id"],
                    source_location=dict(version=1, sources=list(location.sources)),
                    media_type=record["media_type"],
                    incarnation=job["incarnation"],
                )
            )
        return rows

    def reindex(self, timeout: float | None = None) -> ReindexReport:
        with self._call():
            self._validate_timeout(timeout)
            desired_dense = self._provided_dense() or self._dense_config()
            if desired_dense is not None and self._provided_dense() != desired_dense:
                raise CapabilityUnavailable("reindex requires the configured Embedder")
            identity = DocumentId("", "")
            with self._mutation_lock, self._condition:
                job = dict(
                    revision=uuid.uuid4().hex,
                    kind="rebuild",
                    stage="rebuild",
                    state="pending",
                    attempts=0,
                    failures=0,
                    next_run=0,
                    error=None,
                    config=index_config(
                        cast(dict[str, object], self._chunker_description), desired_dense
                    ),
                )
                self._store_job(identity, job)
                self._state = "dirty"
            deadline = None if timeout is None else time.monotonic() + timeout
            with self._condition:
                while not self._is_ready():
                    if self._stopping:
                        raise Closed("MFS is closing")
                    control = self._targets[identity]
                    candidates = (
                        self._targets.values() if control["state"] == "succeeded" else [control]
                    )
                    failures = [
                        j for j in candidates if j["state"] in ("failed", "blocked", "cancelled")
                    ]
                    if failures:
                        raise IndexFailed(str(failures[0].get("error") or failures[0]["state"]))
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        raise WaitTimeout("reindex did not finish before timeout")
                    self._condition.wait(remaining)
            return ReindexReport(
                self._catalog.document_count(),
                len(self._index.scan()),
                self._dense_config() is not None,
            )

    def _rebuild_job(self, identity: DocumentId, job: dict[str, Any]) -> None:
        config = cast(dict[str, object], job["config"])
        dense = config.get("dense")
        dimension = int(cast(dict[str, Any], dense)["dimension"]) if dense else None
        marker = self._path / "INDEX_DIRTY"
        with marker.open("w") as stream:
            stream.write("rebuild\n")
            stream.flush()
            os.fsync(stream.fileno())
        self._fsync_directory(self._path)
        self._index.recreate(dense_dimension=dimension)
        self._write_index_config(config)
        with self._condition:
            updates: list[tuple[DocumentId, dict[str, Any]]] = []
            with self._catalog.transaction():
                for target_id, previous in self._targets.items():
                    if (
                        target_id == identity
                        or previous["stage"] == "process"
                        or previous["state"] == "cancelled"
                    ):
                        continue
                    target = copy.deepcopy(previous)
                    if target["kind"] == "upsert":
                        target.update(
                            stage="chunk",
                            state="pending",
                            vectors=[],
                            error=None,
                            failures=0,
                            next_run=0,
                        )
                    else:
                        target.update(state="succeeded", indexed_revision=None)
                    self._catalog.put_target(target_id.namespace, target_id.doc_id, target)
                    updates.append((target_id, target))
                job.update(state="succeeded", indexed_revision=job["revision"])
                self._catalog.put_target(identity.namespace, identity.doc_id, job)
            for target_id, target in updates:
                self._remember(target_id, target)
            self._remember(identity, job)
            self._config = config
            self._state = "ready"
            self._condition.notify_all()
        marker.unlink()
        self._fsync_directory(self._path)

    def _write_index_config(self, value: dict[str, object]) -> None:
        self._write_json(self._path / "index.json", value)

    def _write_artifact(self, name: str, value: Any) -> str:
        revision = name.removesuffix("-snapshot") if name.endswith("-snapshot") else None
        relative = "artifacts/" + name + "-" + uuid.uuid4().hex + ".json"
        with self._catalog.transaction():
            self._catalog.register_artifact(relative)
        self._write_json(self._path / relative, value)
        if revision is not None:
            with self._condition, self._catalog.transaction():
                identity = value.get("identity")
                current = self._targets.get(DocumentId(**identity)) if identity else None
                if current is not None and (
                    current["revision"] != revision
                    or current["state"] == "cancelled"
                    or current.get("attempt_token") != value.get("preparation_attempt")
                ):
                    current = None
                if current is not None:
                    self._catalog.connection.execute(
                        "INSERT INTO prepared VALUES(?,?) ON CONFLICT(revision) "
                        "DO UPDATE SET path=excluded.path",
                        (revision, relative),
                    )
                    self._catalog.set_references(
                        "prepared", "", revision, {relative, *self._catalog.references(value)}
                    )
        return relative

    def _write_json(self, path: Path, value: Any) -> None:
        temporary = path.parent / ("." + uuid.uuid4().hex + ".tmp")
        try:
            with temporary.open("x", encoding="utf-8") as stream:
                stream.write(compact_json(value))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            self._fsync_directory(path.parent)
        except OSError as error:
            raise StorageFailed(f"failed to persist {path.name}: {error}") from error
        finally:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()

    def _read_artifact(self, relative: str) -> Any:
        path = self._path / relative
        if path.parent != self._path / "artifacts" or path.is_symlink():
            raise CorruptState("artifact path escapes managed storage")
        try:
            return load_json(path.read_text("utf-8"))
        except (OSError, ValueError) as error:
            raise CorruptState(f"cannot read artifact: {error}") from error

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        from ._platform import fsync_directory

        fsync_directory(path)

    def _recover_objects(self) -> None:
        # Migration rebuilds strong roots before the maintenance thread can run.
        # Unowned files are discovered later, incrementally, with a fresh grace period.
        with self._catalog.transaction():
            for ns, doc, record in self._catalog.list_documents():
                self._catalog.set_references("document", ns, doc, self._catalog.references(record))
            for identity, job in self._targets.items():
                references = self._catalog.references(job)
                if job["kind"] == "upsert" and job["stage"] == "process":
                    completed = "artifacts/" + job["revision"] + "-snapshot.json"
                    if (self._path / completed).exists():
                        references.add(completed)
                        references.update(self._catalog.references(self._read_artifact(completed)))
                self._catalog.set_references(
                    "target", identity.namespace, identity.doc_id, references
                )
                self._catalog.connection.execute(
                    "INSERT OR IGNORE INTO runs(revision,state,error) VALUES(?,?,?)",
                    (job["revision"], job["state"], job.get("error")),
                )
            for (name,) in self._catalog.connection.execute(
                "SELECT DISTINCT path FROM artifact_refs"
            ):
                path = self._path / name
                if (
                    path.parent not in (self._path / "objects", self._path / "artifacts")
                    or path.is_symlink()
                    or not path.is_file()
                ):
                    raise CorruptState(f"missing or unsafe managed artifact {name!r}")

    def query(
        self, filters: Sequence[Filter] = (), select: Select = "doc_id", limit: int | None = None
    ) -> QueryResult[Any]:
        with self._call():
            self._validate_query_options(select, limit, search=False)
            documents = self._filter_documents(filters)
            items: list[QueryItem[Any]] = []
            for item in documents:
                if select == "doc_id":
                    items.append(QueryItem(item.id, item.matches))
                elif select == "doc":
                    items.append(QueryItem(self._document(item.id, item.record), item.matches))
                else:
                    # Chunk projection is computed from the SQLite snapshot, independent of Milvus.
                    with self._chunker_lock:
                        ranges = validate_chunk_ranges(
                            item.record["text"],
                            self._chunker.chunk(item.record["text"], self._source_map(item.record)),
                        )
                    encoded = item.record["text"].encode()
                    for ordinal, span in enumerate(ranges):
                        matches = tuple(
                            m
                            for m in item.matches
                            if m.text_start < span.text_end and span.text_start < m.text_end
                        )
                        if item.matches and not matches:
                            continue
                        chunk = Chunk(
                            item.id,
                            item.record["snapshot_id"],
                            ordinal,
                            encoded[span.text_start : span.text_end].decode(),
                            span.text_start,
                            span.text_end,
                            self._source_location(
                                self._source_map(item.record), span.text_start, span.text_end
                            ),
                        )
                        items.append(QueryItem(chunk, matches))
            truncated = limit is not None and len(items) > limit
            return QueryResult(tuple(items if limit is None else items[:limit]), truncated)

    def _filter_documents(self, filters: Sequence[Filter]) -> list[_FilteredDocument]:
        with self._condition:
            names: dict[str, NamespaceKind] = {
                n: cast(NamespaceKind, r["kind"]) for n, r in self._namespaces.items()
            }
        compiled = compile_filters(filters, names, search=False)
        for item in compiled.text:
            self._text_matches(
                "", item
            )  # Invalid patterns fail even when there are no candidate documents.
        result: list[_FilteredDocument] = []
        for ns, doc, record in self._catalog.select_documents(compiled.sql, compiled.params):
            ranges: list[tuple[int, int]] = []
            for text_filter in compiled.text:
                matches = self._text_matches(record["text"], text_filter)
                if not matches:
                    break
                ranges.extend(matches)
            else:
                source_map = self._source_map(record)
                result.append(
                    _FilteredDocument(
                        DocumentId(ns, doc),
                        record,
                        tuple(
                            Match(start, end, self._source_location(source_map, start, end))
                            for start, end in _merge_ranges(ranges)
                        ),
                    )
                )
        return result

    @staticmethod
    def _text_matches(text: str, text_filter: TextMatch) -> list[tuple[int, int]]:
        pattern = text_filter.pattern
        if not isinstance(_runtime(pattern), str) or not pattern or len(pattern.encode()) > 16384:
            raise InvalidFilter("TextMatch pattern must be 1..16384 UTF-8 bytes")
        try:
            sensitive = text_filter.case_sensitive or (
                text_filter.smart_case and any(c.isupper() for c in pattern)
            )
            ranges = regex_ranges(text, pattern, regex=text_filter.regex, case_sensitive=sensitive)
        except Exception as error:
            raise InvalidPattern(f"invalid RE2 pattern: {error}") from error
        if text_filter.whole_word:

            def word(character: str) -> bool:
                return character == "_" or character.isalnum()

            ranges = [
                (start, end)
                for start, end in ranges
                if (start == 0 or not word(text[start - 1]))
                and (end == len(text) or not word(text[end]))
            ]
        offsets = [0]
        for character in text:
            offsets.append(offsets[-1] + len(character.encode()))
        return [(offsets[start], offsets[end]) for start, end in ranges]

    @staticmethod
    def _validate_query_options(select: str, limit: int | None, *, search: bool) -> None:
        if select not in ("doc_id", "chunk", "doc"):
            raise InvalidQuery(f"invalid select {select!r}")
        if limit is not None and (
            isinstance(_runtime(limit), bool) or not isinstance(_runtime(limit), int)
        ):
            raise InvalidQuery("limit must be an integer")
        if search and (limit is None or not 1 <= limit <= 1000):
            raise InvalidQuery("search limit must be 1..1000")
        if not search and limit is not None and not 1 <= limit <= 100000:
            raise InvalidQuery("query limit must be 1..100000 or None")

    def search(
        self,
        text: str,
        filters: Sequence[Filter] = (),
        mode: SearchMode = "hybrid",
        select: Select = "chunk",
        limit: int = 10,
        *,
        consistency: Consistency = "strong",
        timeout: float | None = None,
    ) -> SearchResult[Any]:
        with self._call():
            if not isinstance(_runtime(text), str) or not text or len(text.encode()) > 65536:
                raise InvalidQuery("search text must be 1..65536 UTF-8 bytes")
            if mode not in ("bm25", "vector", "hybrid") or consistency not in (
                "strong",
                "eventual",
            ):
                raise InvalidQuery("invalid search mode or consistency")
            self._validate_query_options(select, limit, search=True)
            self._validate_timeout(timeout)
            if select == "doc":
                raise InvalidQuery(
                    "ranked search returns chunk/doc_id; read documents separately with query"
                )
            with self._condition:
                names: dict[str, NamespaceKind] = {
                    n: cast(NamespaceKind, r["kind"]) for n, r in self._namespaces.items()
                }
            expressions = compile_filters(filters, names, search=True).expressions
            vector = self._embed_query(text) if mode in ("vector", "hybrid") else None
            if consistency == "strong":
                self._wait_ready(timeout)
            candidate_limit = min(1000, max(100, limit * 10))
            channels: list[list[SearchHit]] = []
            while True:
                channels = []
                more = False
                if mode in ("bm25", "hybrid"):
                    hits, extra = self._index.search(
                        text, mode="bm25", expressions=expressions, limit=candidate_limit
                    )
                    channels.append(hits)
                    more |= extra
                if vector is not None:
                    hits, extra = self._index.search(
                        vector, mode="vector", expressions=expressions, limit=candidate_limit
                    )
                    channels.append(hits)
                    more |= extra
                ranked = _rrf(channels) if mode == "hybrid" else channels[0]
                if (
                    select == "chunk"
                    or len({(h["namespace"], h["doc_id"]) for h in ranked}) >= limit
                    or not more
                    or candidate_limit == 1000
                ):
                    break
                candidate_limit = min(1000, candidate_limit * 2)
            items: list[SearchItem[Any]] = []
            seen: set[DocumentId] = set()
            for hit in ranked:
                identity = DocumentId(hit["namespace"], hit["doc_id"])
                if select == "doc_id":
                    if identity in seen:
                        continue
                    seen.add(identity)
                    value: Any = identity
                else:
                    location = hit["source_location"]
                    value = Chunk(
                        identity,
                        hit["snapshot_id"],
                        hit["ordinal"],
                        hit["text"],
                        hit["text_start"],
                        hit["text_end"],
                        SourceLocation(1, tuple(copy_json(v) for v in location["sources"])),
                    )
                items.append(SearchItem(value, hit["score"], ()))
            return SearchResult(tuple(items[:limit]), more or len(items) > limit)

    def sync(
        self, namespace: str, path: str = ".", *, verify: Literal["stat", "content"] = "stat"
    ) -> SyncReport:
        with self._call(), self._mutation_lock:
            return self._sync(namespace, path, verify=verify)

    def _sync(self, namespace: str, path: str, *, verify: str, force: bool = False) -> SyncReport:
        from ._sync import sync_namespace

        return sync_namespace(self, namespace, path, verify=verify, force=force)

    def _excluded(self, relative: str) -> bool:
        parts = relative.split("/")
        return any(
            _glob_match(pattern.rstrip("/"), "/".join(parts[:end]))
            for end in range(1, len(parts) + 1)
            for pattern in self._sync_policy.exclude_globs
        )

    def _stage_descriptor(self, descriptor: int) -> _Staged:
        directory = self._new_staging()
        output = directory / "input"
        try:
            for _ in range(2):
                before = os.fstat(descriptor)
                before_change = descriptor_change_time(descriptor)
                os.lseek(descriptor, 0, os.SEEK_SET)
                digest = blake3.blake3()
                with output.open("wb") as stream:
                    while block := os.read(descriptor, 1024 * 1024):
                        stream.write(block)
                        digest.update(block)
                    stream.flush()
                    os.fsync(stream.fileno())
                after = os.fstat(descriptor)
                if (before.st_size, before.st_mtime_ns, before_change) == (
                    after.st_size,
                    after.st_mtime_ns,
                    descriptor_change_time(descriptor),
                ):
                    return _Staged(
                        directory, output, digest.hexdigest(), after.st_size, after.st_mtime_ns
                    )
            raise SourceChanged("file changed during stable read")
        except Exception:
            self._remove_staging(directory)
            raise

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
                        before_change = descriptor_change_time(input_stream.fileno())
                        while block := input_stream.read(1024 * 1024):
                            output_stream.write(block)
                            digest.update(block)
                        output_stream.flush()
                        os.fsync(output_stream.fileno())
                        after = os.fstat(input_stream.fileno())
                        after_change = descriptor_change_time(input_stream.fileno())
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
                    if (
                        identity_before == identity_after == path_identity
                        and before_change == after_change
                    ):
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

    def _select_processor(
        self,
        doc_id: str,
        staged_path: Path,
        explicit_media_type: str | None,
        fallback_path: Path | None,
        *,
        head: bytes | None = None,
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
            if head is None:
                with staged_path.open("rb") as stream:
                    head = stream.read(64 * 1024)
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

    def _object_path(self, record: dict[str, Any] | None) -> Path | None:
        if record is None:
            return None
        try:
            value = record["source"]["object"]
            if value is None:
                return None
            candidate = self._path / str(value)
            if (
                candidate.parent.resolve() != (self._path / "objects").resolve()
                or candidate.is_symlink()
            ):
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
