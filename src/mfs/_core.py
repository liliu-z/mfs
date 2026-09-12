# pyright: reportPrivateUsage=false
from __future__ import annotations

import contextlib
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
from ._indexing import Indexing
from ._json import JSONValue, copy_json, load_json
from ._lifecycle import Lifecycle, ReadView
from ._locks import CallGate
from ._namespace import NamespaceBinding
from ._platform import descriptor_change_time
from ._preparation import Preparation
from ._reader import Reader
from ._rules import excluded, validate_rules
from ._runtime import NamespaceRuntime
from ._validation import (
    normalized_media_type,
    suffix_for,
    validate_external_path,
    validate_internal_id,
    validate_namespace,
)
from ._work import SourceInput
from ._worker import Worker
from .errors import (
    CapabilityUnavailable,
    Closed,
    CorruptState,
    IndexFailed,
    IndexUnavailable,
    InstanceLocked,
    InvalidConfiguration,
    InvalidQuery,
    MFSError,
    MigrationRequired,
    NamespaceCompatibilityError,
    NamespaceConflict,
    NamespaceNotFound,
    OperationFailed,
    ProcessingFailed,
    RootOverlap,
    SourceChanged,
    SourceExcluded,
    SourceUnavailable,
    StorageFailed,
    UnsupportedMediaType,
    WaitTimeout,
    WrongNamespaceKind,
)
from .types import (
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
    GrepBudget,
    GrepResult,
    IgnoreRule,
    IndexingMode,
    IndexState,
    MutationReport,
    NamespaceConfiguration,
    NamespaceInfo,
    NamespaceKind,
    Processor,
    Progress,
    ReindexReport,
    RuleSet,
    ScopeStatus,
    SearchMode,
    SearchResult,
    Select,
    Status,
    SyncPolicy,
    SyncReport,
    TaskError,
    TaskStage,
    TaskState,
    UnderPath,
)


@dataclass(slots=True)
class _Staged:
    directory: Path | None
    path: Path
    content_hash: str
    size: int
    mtime_ns: int | None


class MFS:
    @classmethod
    def open(cls, mfs_path: Path, *, gc_policy: GCPolicy | None = None) -> MFS:
        return cls(mfs_path, gc_policy=gc_policy)

    def __init__(self, mfs_path: Path, *, gc_policy: GCPolicy | None = None) -> None:
        self._path = Path(mfs_path).expanduser().resolve()
        self._condition = threading.Condition(threading.RLock())
        self._mutation_lock = threading.RLock()
        self._gc_policy = gc_policy or GCPolicy()
        for key, value in asdict(self._gc_policy).items():
            if key != "enabled" and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise InvalidConfiguration(f"invalid GC policy value: {key}")
        for count in (self._gc_policy.batch_files, self._gc_policy.cycle_files):
            if (
                isinstance(_runtime(count), bool)
                or not isinstance(_runtime(count), int)
                or count < 1
            ):
                raise InvalidConfiguration("GC file budgets must be positive integers")
        if self._gc_policy.interval <= 0:
            raise InvalidConfiguration("GC interval must be positive")
        self._calls = CallGate()
        self._stopping = False
        self._state: IndexState = "ready"
        self._workers: list[threading.Thread] = []
        from ._search_execution import SearchExecution

        self._search_execution = SearchExecution()
        try:
            self._path.mkdir(parents=True, exist_ok=True)
            initialize = not any(self._path.iterdir())
            if not initialize and not (self._path / "catalog.sqlite").is_file():
                raise CorruptState("non-empty mfs_path has no recognizable catalog")
            self._instance_lock = FileLock(self._path / "LOCK", thread_local=False)
            try:
                self._instance_lock.acquire(timeout=0)
            except Timeout as error:
                raise InstanceLocked(f"MFS instance is already open: {self._path}") from error
            for name in ("objects", "artifacts", "staging", "work", "namespaces"):
                (self._path / name).mkdir(exist_ok=True)
            self._catalog = Catalog(self._path / "catalog.sqlite", initialize=initialize)
            self._tasks = Lifecycle(self._catalog, self._condition)
            self._artifacts = ArtifactStore(self._path, self._catalog, self._tasks, self._gc_policy)
            self._runtime = NamespaceRuntime(self._path, self._tasks)
            self._preparation = Preparation(
                self._path, self._catalog, self._artifacts, self._tasks, self._runtime
            )
            for name in self._tasks.namespaces:
                if "manifest" not in self._tasks.namespaces[name]:
                    continue
                index = self._runtime.index(name)
                dense = self._runtime.dense_config(name)
                if index.has_valid_collection(
                    dense_dimension=int(dense["dimension"]) if dense else None
                ):
                    index.load()
                else:
                    self._runtime.index_errors.add(name)
            self._view = ReadView(self._tasks, self._runtime.index_errors)
            self._reader = Reader(self._catalog, self._artifacts, self._runtime, self._view)
            self._recover_objects()
            indexing = Indexing(self._catalog, self._tasks, self._runtime, self._preparation)
            self._worker = Worker(self._tasks, self._runtime, self._preparation, indexing)
            worker = threading.Thread(target=self._worker.run, name="mfs-worker")
            worker.start()
            self._workers.append(worker)
            if self._gc_policy.enabled:
                maintenance = threading.Thread(
                    target=self._artifacts.maintain, name="mfs-maintenance"
                )
                maintenance.start()
                self._workers.append(maintenance)
        except Exception:
            self._stopping = True
            self._search_execution.stop()
            with self._condition:
                if hasattr(self, "_tasks"):
                    self._tasks.stop()
                self._condition.notify_all()
            for thread in self._workers:
                thread.join()
            self._search_execution.close()
            for name in ("_runtime", "_catalog"):
                with contextlib.suppress(Exception):
                    getattr(self, name).close()
            with contextlib.suppress(Exception):
                self._instance_lock.release()
            raise

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

    def _is_ready(self) -> bool:
        return self._state == "ready" and not self._tasks.pending

    @contextlib.contextmanager
    def _call(self, *, activity: bool = True) -> Generator[None]:
        with self._calls.call():
            with self._condition:
                if activity:
                    self._tasks.readers += 1
                    self._tasks.last_activity = time.monotonic()
            try:
                yield
            finally:
                with self._condition:
                    if activity:
                        self._tasks.readers -= 1
                        self._tasks.last_activity = time.monotonic()

    def close(self) -> None:
        self._search_execution.stop()
        self._tasks.stop()
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
        if not self._calls.begin_close():
            return
        try:
            for thread in self._workers:
                thread.join()
            self._search_execution.close()
            try:
                self._runtime.close()
            finally:
                self._catalog.close()
                self._instance_lock.release()
        finally:
            self._calls.finish_close()

    def wait(
        self,
        target: DocumentId | str | MutationReport | DropReport | SyncReport,
        timeout: float | None = None,
        *,
        path: str = ".",
    ) -> None:
        """Wait for current file/scope work, following updates accepted while waiting.

        Reports are shorthand for their file or namespace/path, not historical receipts.
        Failed, blocked and cancelled current work raises OperationFailed.
        """
        self._validate_timeout(timeout)
        dropping = isinstance(target, DropReport)
        if isinstance(target, SyncReport):
            if not target.complete:
                raise OperationFailed("sync observation was incomplete", state="incomplete")
            target, path = target.namespace, target.path
        elif isinstance(target, MutationReport):
            target = target.id
        elif isinstance(target, DropReport):
            target = target.namespace
        namespace = target.namespace if isinstance(target, DocumentId) else target
        validate_namespace(namespace)
        if path != ".":
            path = validate_external_path(path, allow_root=True)
        with self._call(activity=False):
            with self._condition:
                if (
                    not dropping
                    and namespace not in self._tasks.namespaces
                    and not any(i.namespace == namespace for i in self._tasks.targets)
                ):
                    raise NamespaceNotFound(f"namespace {namespace!r} does not exist")
            self._tasks.wait(
                namespace, target if isinstance(target, DocumentId) else None, path, timeout
            )

    def index_configuration(self, namespace: str) -> JSONValue:
        with self._call(activity=False), self._condition:
            self._required_namespace(namespace)
            return copy_json(self._tasks.namespaces[namespace]["manifest"]["index"])

    def namespace_configuration(self, namespace: str) -> NamespaceConfiguration:
        """Return a detached snapshot of persisted settings without loading adapters."""
        with self._call(activity=False), self._condition:
            info = self._required_namespace(namespace)
            self._require_modern_namespace(namespace)
            record = self._tasks.namespaces[namespace]
            return NamespaceConfiguration(
                namespace,
                info.kind,
                info.root,
                record["indexing"],
                record["paused"],
                copy_json(record["manifest"]),
                copy_json(record.get("pending_manifest")),
                record.get("max_file_bytes"),
            )

    def scope_status(self, namespace: str | None = None, path: str = ".") -> ScopeStatus:
        with self._call(activity=False), self._condition:
            states, stages = self._catalog.status_counts(namespace, path)
            return ScopeStatus(sum(states.values()), states, stages)

    @staticmethod
    def _under(doc_id: str, path: str) -> bool:
        return path == "." or doc_id == path or doc_id.startswith(path + "/")

    def set_active_scopes(self, scopes: Sequence[UnderPath]) -> None:
        with self._call(activity=False), self._condition:
            for scope in scopes:
                self._required_namespace(scope.namespace)
            self._tasks.active_scopes = tuple(scopes)
            self._condition.notify_all()

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
                path = self._artifacts.path(artifacts[name])
            return ArtifactHandle(
                path.open("rb"), str(row[0]), lambda: lease.__exit__(None, None, None)
            )
        except BaseException:
            lease.__exit__(None, None, None)
            raise

    def collect_garbage(self) -> GCReport:
        # Lifecycle pin only: this call must not count as foreground artifact use.
        with self._calls.call():
            return self._artifacts.collect()

    def garbage_collection_status(self) -> GCReport:
        with self._calls.call():
            return self._artifacts.last_report

    def wait_ready(self, timeout: float | None = None) -> None:
        self._validate_timeout(timeout)
        with self._call(activity=False):
            self._view.wait_ready(timeout)

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
        self,
        namespace: str,
        kind: NamespaceKind,
        root: Path | None = None,
        *,
        processors: Sequence[Processor] = (),
        chunker: Chunker | None = None,
        embedder: Embedder | None = None,
        indexing: IndexingMode | None = None,
        sync_policy: SyncPolicy | None = None,
        ignore_rules: Sequence[IgnoreRule] = (),
    ) -> NamespaceInfo:
        with self._call(), self._mutation_lock:
            validate_namespace(namespace)
            if kind not in ("internal", "external"):
                raise InvalidConfiguration("namespace kind must be internal or external")
            mode: IndexingMode = indexing or ("hybrid" if embedder else "bm25")
            binding = NamespaceBinding.build(processors, chunker, embedder, mode)
            policy = self._validate_sync_policy(sync_policy or SyncPolicy())
            rules = validate_rules(
                [
                    *(IgnoreRule(f"exclude-{i}", p) for i, p in enumerate(policy.exclude_globs)),
                    *ignore_rules,
                ]
            )
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
                previous = self._tasks.namespaces.get(namespace)
                if previous is not None:
                    if previous["kind"] != kind or previous["root"] != (
                        str(spelling) if spelling else None
                    ):
                        raise NamespaceConflict(f"namespace {namespace!r} has another binding")
                    raise NamespaceConflict(
                        f"namespace {namespace!r} already exists; use open_namespace"
                    )
                record: dict[str, Any] = dict(
                    version=4,
                    kind=kind,
                    root=str(spelling) if spelling else None,
                    root_actual=str(actual) if actual else None,
                    incarnation=uuid.uuid4().hex,
                    binding=uuid.uuid4().hex,
                    manifest=binding.manifest,
                    indexing=mode,
                    paused=False,
                    rules=[asdict(r) for r in rules],
                    rules_revision=uuid.uuid4().hex,
                    max_file_bytes=policy.max_file_bytes,
                )
                index = self._runtime.index(namespace, record["incarnation"])
                dense = binding.manifest["index"]["dense"]
                index.recreate(dense_dimension=dense["dimension"] if dense else None)
                self._tasks.configure(namespace, record)
                self._runtime.bindings[namespace] = binding
                self._condition.notify_all()
                return self._namespace_info(namespace, record)

    def _require_modern_namespace(self, namespace: str) -> None:
        self._required_namespace(namespace)
        if "manifest" not in self._tasks.namespaces[namespace]:
            raise MigrationRequired(
                f"{namespace}: call migrate_namespace with adapters and indexing mode"
            )

    def migrate_namespace(
        self,
        namespace: str,
        *,
        processors: Sequence[Processor],
        indexing: IndexingMode,
        chunker: Chunker | None = None,
        embedder: Embedder | None = None,
        ignore_rules: Sequence[IgnoreRule] = (),
    ) -> NamespaceInfo:
        from ._migration import migrate

        with self._call(), self._mutation_lock:
            self._required_namespace(namespace)
            binding = NamespaceBinding.build(processors, chunker, embedder, indexing)
            return migrate(self, namespace, binding, indexing, tuple(ignore_rules))

    def open_namespace(
        self,
        namespace: str,
        *,
        processors: Sequence[Processor] = (),
        chunker: Chunker | None = None,
        embedder: Embedder | None = None,
    ) -> NamespaceInfo:
        with self._call(), self._mutation_lock:
            self._required_namespace(namespace)
            self._require_modern_namespace(namespace)
            record = self._tasks.namespaces[namespace]
            binding = NamespaceBinding.build(processors, chunker, embedder, record["indexing"])
            binding.verify(namespace, record.get("pending_manifest", record["manifest"]))
            if namespace in self._runtime.index_errors and "pending_manifest" not in record:
                raise IndexUnavailable(
                    f"{namespace}: collection is missing or incompatible; explicit reindex required"
                )
            with self._condition:
                self._runtime.bindings[namespace] = binding
                self._tasks.resume_blocked(namespace)
                self._condition.notify_all()
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
        record = self._tasks.namespaces.get(namespace)
        if record is None:
            raise NamespaceNotFound(f"namespace {namespace!r} does not exist")
        return self._namespace_info(namespace, record)

    def get_namespace(self, namespace: str) -> NamespaceInfo:
        with self._call(), self._condition:
            return self._required_namespace(namespace)

    def list_namespaces(self) -> tuple[NamespaceInfo, ...]:
        with self._call(), self._condition:
            return tuple(
                self._namespace_info(n, self._tasks.namespaces[n])
                for n in sorted(self._tasks.namespaces, key=str.encode)
            )

    def drop_namespace(self, namespace: str) -> DropReport:
        with self._call(), self._mutation_lock, self._condition:
            report = self._tasks.drop_namespace(namespace)
            self._runtime.bindings.pop(namespace, None)
            self._runtime.index_errors.discard(namespace)
            return report

    def status(self) -> Status:
        with self._call(activity=False), self._condition:
            ready = self._is_ready() and not self._runtime.index_errors
            enabled = [
                ns for ns in self._tasks.namespaces if self._runtime.dense_config(ns) is not None
            ]
            return Status(
                self._catalog.namespace_count(),
                self._catalog.document_count(),
                "dirty" if self._runtime.index_errors else "ready" if ready else "pending",
                bool(enabled),
                any(ns in self._runtime.bindings for ns in enabled),
                ready,
                len(self._tasks.pending),
                sum(j["state"] in ("failed", "blocked") for j in self._tasks.targets.values()),
            )

    def document_status(self, document_id: DocumentId) -> DocumentStatus | None:
        with self._call(activity=False), self._condition:
            job = self._tasks.targets.get(document_id)
            if job is None:
                return None
            text_revision = self._catalog.get_document_revision(
                document_id.namespace, document_id.doc_id
            )
            progress = self._tasks.progress.get(document_id, job.get("progress"))
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
                int(job.get("completed_batches", 0)),
                int(job.get("batches", 0)),
                any(identity == document_id for identity, _ in self._tasks.executing),
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
            ids = self._catalog.status_ids(namespace, path, limit, offset)
            return tuple(
                status
                for ns, doc in ids
                if (status := self.document_status(DocumentId(ns, doc))) is not None
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
            if self._excluded(namespace, doc_id):
                raise SourceExcluded(f"{namespace}:{doc_id} is excluded")
            staged = (
                self._stage_bytes(data, namespace)
                if isinstance(data, bytes)
                else self._stage_path(Path(data), namespace)
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
        media, description = self._select_processor(
            identity.namespace, identity.doc_id, staged.path, media_type, fallback
        )
        if idempotency_key is not None and (
            not isinstance(_runtime(idempotency_key), str)
            or not idempotency_key
            or len(idempotency_key.encode()) > 2048
        ):
            raise InvalidQuery("idempotency_key must be a non-empty string of at most 2048 bytes")
        return self._tasks.accept(
            identity,
            SourceInput(
                staged.path, staged.content_hash, staged.size, staged.mtime_ns, media, description
            ),
            self._artifacts,
            idempotency_key=idempotency_key,
            force=force,
        )

    def remove(self, namespace: str, doc_id: str) -> MutationReport:
        with self._call(), self._mutation_lock:
            validate_internal_id(doc_id)
            if self._required_namespace(namespace).kind != "internal":
                raise WrongNamespaceKind(
                    "remove requires an internal namespace; use sync for external files"
                )
            return self._tasks.remove(DocumentId(namespace, doc_id))

    def retry(self, document_id: DocumentId, stage: TaskStage | None = None) -> None:
        with self._call():
            self._tasks.retry(document_id, stage)

    def cancel(self, document_id: DocumentId) -> None:
        with self._call():
            self._tasks.cancel(document_id)

    def reprocess(self, document_id: DocumentId) -> MutationReport:
        with self._call(), self._mutation_lock:
            info = self._required_namespace(document_id.namespace)
            if info.kind == "external":
                report = self._sync(
                    document_id.namespace, document_id.doc_id, verify="content", force=True
                )
                if report.failed or report.skipped or not report.changed:
                    raise SourceUnavailable("external input could not be reprocessed")
                job = self._tasks.targets[document_id]
                return MutationReport(document_id, "updated", False, job["revision"])
            with self._condition:
                job = self._tasks.targets.get(document_id)
                if job is None or job["kind"] != "upsert":
                    raise InvalidQuery("document does not exist")
                input_name = job.get("input") or job.get("source", {}).get("object")
            staged = self._stage_path(self._path / str(input_name), document_id.namespace)
            try:
                return self._admit(document_id, staged, media_type=job["media_type"], force=True)
            finally:
                self._remove_staging(staged.directory)

    def reindex(
        self,
        namespace: str,
        timeout: float | None = None,
        *,
        chunker: Chunker | None = None,
        embedder: Embedder | None = None,
        processors: Sequence[Processor] | None = None,
        indexing: IndexingMode | None = None,
    ) -> ReindexReport:
        with self._call():
            return self._reindex(
                namespace,
                timeout,
                chunker=chunker,
                embedder=embedder,
                processors=processors,
                indexing=indexing,
            )

    def _reindex(
        self,
        namespace: str,
        timeout: float | None = None,
        *,
        chunker: Chunker | None = None,
        embedder: Embedder | None = None,
        processors: Sequence[Processor] | None = None,
        indexing: IndexingMode | None = None,
    ) -> ReindexReport:
        with self._mutation_lock:
            self._validate_timeout(timeout)
            self._required_namespace(namespace)
            old = self._runtime.bindings.get(namespace)
            if old is None and processors is None:
                raise CapabilityUnavailable("reindex requires namespace adapters")
            record = self._tasks.namespaces[namespace]
            binding = NamespaceBinding.build(
                processors if processors is not None else old.processors if old else (),
                chunker or (old.chunker if old else None),
                embedder or (old.embedder if old else None),
                indexing or record["indexing"],
            )
            if binding.manifest["processors"] != record["manifest"]["processors"]:
                raise NamespaceCompatibilityError(
                    "reindex cannot change Processors; use reprocess_namespace"
                )
            self._request_rebuild(namespace, binding, {"indexing": indexing or record["indexing"]})
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while any(i.namespace == namespace for i in self._tasks.pending):
                if self._stopping:
                    raise Closed("MFS instance is closing")
                control = self._tasks.targets[DocumentId(namespace, "")]
                candidates = (
                    self._tasks.targets.items()
                    if control["state"] == "succeeded"
                    else [(DocumentId(namespace, ""), control)]
                )
                for i, j in candidates:
                    if i.namespace == namespace and j["state"] in (
                        "failed",
                        "blocked",
                        "cancelled",
                    ):
                        raise IndexFailed(str(j.get("error") or j["state"]))
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise WaitTimeout("reindex did not finish before timeout")
                self._condition.wait(remaining)
        return ReindexReport(
            self._catalog.document_count(namespace),
            len(self._runtime.index(namespace).scan()),
            self._runtime.dense_config(namespace) is not None,
        )

    def _request_rebuild(
        self, namespace: str, binding: NamespaceBinding, changes: dict[str, Any] | None = None
    ) -> None:
        with self._condition:
            self._tasks.request_rebuild(namespace, binding.manifest, changes)
            self._runtime.bindings[namespace] = binding

    def _recover_objects(self) -> None:
        # Migration rebuilds strong roots before the maintenance thread can run.
        # Unowned files are discovered later, incrementally, with a fresh grace period.
        with self._catalog.transaction():
            for ns, doc, record in self._catalog.list_documents():
                self._catalog.set_references("document", ns, doc, self._catalog.references(record))
            for identity, job in self._tasks.targets.items():
                references = self._catalog.references(job)
                if job["kind"] == "upsert" and job["stage"] == "process":
                    completed = "artifacts/" + job["revision"] + "-snapshot.json"
                    if (self._path / completed).exists():
                        references.add(completed)
                        references.update(self._catalog.references(self._artifacts.read(completed)))
                self._catalog.set_references(
                    "target", identity.namespace, identity.doc_id, references
                )
            for (name,) in self._catalog.connection.execute(
                "SELECT DISTINCT path FROM artifact_refs"
            ):
                path = self._artifacts.path(name)
                if not path.is_file():
                    raise CorruptState(f"missing or unsafe managed artifact {name!r}")

    def grep(
        self,
        namespace: str,
        filters: Sequence[Filter] = (),
        select: Select = "doc_id",
        limit: int | None = 100,
        *,
        budget: GrepBudget | None = None,
    ) -> GrepResult[Any]:
        with self._call():
            return self._reader.grep(namespace, filters, select, limit, budget or GrepBudget())

    def read(self, document_id: DocumentId) -> Document | None:
        with self._call():
            return self._reader.read(document_id)

    def search(
        self,
        namespace: str,
        text: str,
        filters: Sequence[Filter] = (),
        mode: SearchMode = "hybrid",
        select: Select = "chunk",
        limit: int = 10,
        *,
        consistency: Consistency = "eventual",
        timeout: float | None = 5.0,
    ) -> SearchResult[Any]:
        """Return current results within the caller's total search timeout.

        An adapter already executing may finish after timeout; its result is discarded.
        """
        from ._search_execution import SearchDeadline

        self._validate_timeout(timeout)
        selected_filters = tuple(filters)

        def execute(deadline: SearchDeadline) -> SearchResult[Any]:
            with self._call():
                return self._reader.search(
                    namespace, text, selected_filters, mode, select, limit, consistency, deadline
                )

        with self._calls.call():
            return self._search_execution.run(timeout, execute)

    def sync(
        self, namespace: str, path: str = ".", *, verify: Literal["stat", "content"] = "stat"
    ) -> SyncReport:
        with self._call(), self._mutation_lock:
            return self._sync(namespace, path, verify=verify)

    def _sync(self, namespace: str, path: str, *, verify: str, force: bool = False) -> SyncReport:
        from ._sync import sync_namespace

        return sync_namespace(self, namespace, path, verify=verify, force=force)

    def _excluded(self, namespace: str, relative: str, *, directory: bool = False) -> bool:
        return excluded(
            tuple(IgnoreRule(**r) for r in self._tasks.namespaces[namespace]["rules"]),
            relative,
            directory=directory,
        )

    def rules(self, namespace: str) -> RuleSet:
        with self._call(activity=False), self._condition:
            self._required_namespace(namespace)
            record = self._tasks.namespaces[namespace]
            return RuleSet(
                record["rules_revision"], tuple(IgnoreRule(**r) for r in record["rules"])
            )

    def update_rules(
        self,
        namespace: str,
        *,
        expected_revision: str,
        add: Sequence[IgnoreRule] = (),
        remove: Sequence[str] = (),
        replace: Sequence[IgnoreRule] = (),
        order: Sequence[str] | None = None,
    ) -> RuleSet:
        with self._call(), self._mutation_lock:
            return self._tasks.update_rules(
                namespace,
                expected_revision=expected_revision,
                add=add,
                remove=remove,
                replace=replace,
                order=order,
            )

    def configure_index(
        self,
        namespace: str,
        *,
        indexing: IndexingMode | None = None,
        paused: bool | None = None,
    ) -> None:
        with self._call(), self._mutation_lock, self._condition:
            self._require_modern_namespace(namespace)
            previous = self._tasks.namespaces[namespace]
            if paused is not None and not isinstance(_runtime(paused), bool):
                raise InvalidConfiguration("paused must be a boolean")
            mode = previous["indexing"] if indexing is None else indexing
            changes = dict(indexing=mode, paused=previous["paused"] if paused is None else paused)
            if mode != previous["indexing"]:
                binding = self._runtime.binding(namespace)
                desired = NamespaceBinding.build(
                    binding.processors, binding.chunker, binding.embedder, mode
                )
                self._request_rebuild(namespace, desired, changes)
            else:
                record = dict(previous, **changes)
                self._tasks.configure(namespace, record)
            self._condition.notify_all()

    def reprocess_namespace(
        self, namespace: str, *, processors: Sequence[Processor]
    ) -> SyncReport | tuple[MutationReport, ...]:
        with self._call(), self._mutation_lock:
            binding = self._runtime.binding(namespace)
            previous = self._tasks.namespaces[namespace]
            if "pending_manifest" in previous:
                raise IndexUnavailable(
                    "finish the pending index rebuild before changing Processors"
                )
            desired = NamespaceBinding.build(
                processors, binding.chunker, binding.embedder, previous["indexing"]
            )
            routes = {
                media: desired.descriptions[id(processor)]
                for processor in desired.processors
                for media in desired.media_types[id(processor)]
            }
            with self._condition:
                reports = self._tasks.reprocess_namespace(namespace, desired.manifest, routes)
                self._runtime.bindings[namespace] = desired
            if previous["kind"] == "external":
                return self._sync(namespace, ".", verify="content")
            return reports

    def _stage_descriptor(self, descriptor: int, source: Path) -> _Staged:
        for _ in range(2):
            before = os.fstat(descriptor)
            before_change = descriptor_change_time(descriptor)
            os.lseek(descriptor, 0, os.SEEK_SET)
            digest = blake3.blake3()
            while block := os.read(descriptor, 1024 * 1024):
                digest.update(block)
            after = os.fstat(descriptor)
            if (before.st_size, before.st_mtime_ns, before_change) == (
                after.st_size,
                after.st_mtime_ns,
                descriptor_change_time(descriptor),
            ):
                return _Staged(None, source, digest.hexdigest(), after.st_size, after.st_mtime_ns)
        raise SourceChanged("file changed during observation")

    def _stage_bytes(self, data: bytes, namespace: str) -> _Staged:
        directory = self._new_staging(namespace)
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

    def _stage_path(self, source: Path, namespace: str) -> _Staged:
        directory = self._new_staging(namespace)
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

    def _new_staging(self, namespace: str) -> Path:
        directory = (
            self._artifacts.directory(self._tasks.namespaces[namespace]["incarnation"], "work")
            / uuid.uuid4().hex
        )
        try:
            directory.mkdir()
            return directory
        except OSError as error:
            raise StorageFailed(f"failed to create staging directory: {error}") from error

    @staticmethod
    def _remove_staging(directory: Path | None) -> None:
        if directory is None:
            return
        with contextlib.suppress(OSError):
            shutil.rmtree(directory)

    def _select_processor(
        self,
        namespace: str,
        doc_id: str,
        staged_path: Path,
        explicit_media_type: str | None,
        fallback_path: Path | None,
        *,
        head: bytes | None = None,
    ) -> tuple[str, dict[str, Any]]:
        self._require_modern_namespace(namespace)
        binding = self._runtime.bindings.get(namespace)
        descriptions = self._tasks.namespaces[namespace]["manifest"]["processors"]
        by_media = {
            media: {k: p[k] for k in ("id", "version", "options")}
            for p in descriptions
            for media in p["media_types"]
        }
        by_suffix = {
            suffix: (media, by_media[media])
            for p in descriptions
            for suffix, media in p["suffix_media_types"].items()
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
            if binding is None:
                raise CapabilityUnavailable(
                    "sniffing an unknown suffix requires namespace Processors"
                )
            sniffed: list[tuple[str, dict[str, Any]]] = []
            for processor in binding.processors:
                media_type = processor.sniff(head)
                if media_type is None:
                    continue
                if media_type not in binding.media_types[id(processor)]:
                    processor_id = binding.descriptions[id(processor)]["id"]
                    raise InvalidConfiguration(
                        f"Processor {processor_id!r} sniff returned unowned media type "
                        f"{media_type!r}"
                    )
                sniffed.append((media_type, binding.descriptions[id(processor)]))
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


def _runtime(value: object) -> object:
    return value


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
