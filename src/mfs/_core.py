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
from ._index import ChunkIndex
from ._json import JSONValue, canonical_json, compact_json, copy_json, load_json
from ._lifecycle import Lifecycle
from ._locks import CallGate
from ._namespace import NamespaceBinding
from ._platform import descriptor_change_time
from ._regex import regex_ranges
from ._rules import excluded, validate_rules
from ._validation import (
    normalized_media_type,
    suffix_for,
    validate_internal_id,
    validate_namespace,
)
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
    MigrationRequired,
    NamespaceCompatibilityError,
    NamespaceConflict,
    NamespaceNotFound,
    OperationFailed,
    ProcessingFailed,
    RetryableError,
    RootOverlap,
    RuleConflict,
    SourceChanged,
    SourceExcluded,
    SourceUnavailable,
    StorageFailed,
    Superseded,
    UnsupportedMediaType,
    WaitTimeout,
    WrongNamespaceKind,
)
from .processing import Cancellation, _ProcessingStopped, _ProcessingYielded
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
        self._artifact_readers = 0
        self._last_activity = time.monotonic()
        self._active_scopes: tuple[UnderPath, ...] = ()
        self._chunker_lock = threading.Lock()
        self._calls = CallGate()
        self._stopping = False
        self._state: IndexState = "ready"
        self._transient_text: tuple[str, str] | None = None
        self._bindings: dict[str, NamespaceBinding] = {}
        self._collections: dict[str, ChunkIndex] = {}
        self._index_errors: set[str] = set()
        self._workers: list[threading.Thread] = []
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
            self._artifacts = ArtifactStore(self, self._gc_policy)
            self._index = ChunkIndex(self._path / "milvus.db")
            for name in self._tasks.namespaces:
                if "manifest" not in self._tasks.namespaces[name]:
                    continue
                index = self._namespace_index(name)
                dense = self._dense_config(name)
                if index.has_valid_collection(
                    dense_dimension=int(dense["dimension"]) if dense else None
                ):
                    index.load()
                else:
                    self._index_errors.add(name)
            self._recover_objects()
            worker = threading.Thread(target=self._worker, args=("worker",), name="mfs-worker")
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
            with self._condition:
                if hasattr(self, "_tasks"):
                    for cancellation in self._tasks.cancellations.values():
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

    def _binding(self, namespace: str) -> NamespaceBinding:
        self._require_modern_namespace(namespace)
        binding = self._bindings.get(namespace)
        if binding is None:
            raise CapabilityUnavailable(f"namespace {namespace!r} needs open_namespace binding")
        return binding

    def _namespace_index(self, namespace: str, incarnation: str | None = None) -> ChunkIndex:
        name = "ns_" + (incarnation or self._tasks.namespaces[namespace]["incarnation"])
        if name not in self._collections:
            self._collections[name] = self._index.collection(name)
        return self._collections[name]

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

    def _dense_config(self, namespace: str) -> dict[str, Any] | None:
        return self._tasks.namespaces[namespace].get("manifest", {}).get("index", {}).get("dense")

    def _is_ready(self) -> bool:
        return self._state == "ready" and not self._tasks.pending

    @contextlib.contextmanager
    def _call(self, *, activity: bool = True) -> Generator[None]:
        with self._calls.call():
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
            for cancellation in self._tasks.cancellations.values():
                cancellation._cancel("close")
            self._condition.notify_all()
        if not self._calls.begin_close():
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
            self._calls.finish_close()

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
                revisions = self._catalog.wait_targets(str(row[0]))
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

    def index_configuration(self, namespace: str) -> JSONValue:
        with self._call(activity=False), self._condition:
            self._required_namespace(namespace)
            return copy_json(self._tasks.namespaces[namespace]["manifest"]["index"])

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
        age = max(0, time.time() - enqueued) / 60.0
        return (base - age, enqueued, identity.doc_id)

    def _processor_for(self, job: dict[str, Any]) -> Processor | None:
        binding = self._bindings.get(job.get("identity", {}).get("namespace", ""))
        if binding is None:
            return None
        return next(
            (p for p in binding.processors if binding.descriptions[id(p)] == job.get("processor")),
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
        with self._call(activity=False):
            self._wait_ready(timeout)

    def _wait_ready(self, timeout: float | None, namespaces: set[str] | None = None) -> None:
        self._validate_timeout(timeout)
        with self._condition:
            selected = set(self._tasks.namespaces) if namespaces is None else namespaces
            for namespace in selected:
                self._require_modern_namespace(namespace)
            if any(
                n in self._index_errors and "pending_manifest" not in self._tasks.namespaces[n]
                for n in selected
            ):
                raise IndexUnavailable("selected namespace collection requires explicit reindex")

            def ready() -> bool:
                return not any(
                    namespaces is None or i.namespace in selected for i in self._tasks.pending
                )

            if self._state in ("dirty", "mismatch"):
                raise IndexUnavailable(f"index state is {self._state}; call reindex()")
            completed = self._condition.wait_for(
                lambda: self._stopping or self._state != "ready" or ready(), timeout
            )
            if self._stopping:
                raise Closed("MFS instance is closing")
            if self._state != "ready":
                raise IndexUnavailable(f"index state is {self._state}")
            if not completed:
                raise WaitTimeout(f"{len(self._tasks.pending)} indexing targets have not completed")

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
                for name, record in self._tasks.namespaces.items():
                    if (
                        name != namespace
                        and record["kind"] == "external"
                        and _paths_overlap(actual, Path(record["root"]).resolve())
                    ):
                        raise RootOverlap(f"external root overlaps namespace {name!r}")
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
                index = self._namespace_index(namespace, record["incarnation"])
                dense = binding.manifest["index"]["dense"]
                index.recreate(dense_dimension=dense["dimension"] if dense else None)
                with self._catalog.transaction():
                    self._catalog.put_namespace(namespace, record)
                self._tasks.namespaces[namespace] = record
                self._bindings[namespace] = binding
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
            if namespace in self._index_errors and "pending_manifest" not in record:
                raise IndexUnavailable(
                    f"{namespace}: collection is missing or incompatible; explicit reindex required"
                )
            with self._condition:
                self._bindings[namespace] = binding
                for identity, previous in list(self._tasks.targets.items()):
                    if identity.namespace == namespace and previous["state"] == "blocked":
                        self._tasks.persist(identity, dict(previous, state="pending", error=None))
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
            validate_namespace(namespace)
            if namespace not in self._tasks.namespaces:
                job = self._tasks.targets.get(DocumentId(namespace, ""))
                operation_id = uuid.uuid4().hex
                with self._catalog.transaction():
                    self._catalog.add_wait_operation(
                        operation_id, [str(job["revision"])] if job else []
                    )
                return DropReport(
                    namespace,
                    False,
                    not self._tasks.pending and self._state == "ready",
                    operation_id,
                )
            # One durable namespace cleanup survives an immediate same-name recreation.
            identity = DocumentId(namespace, "")
            job = self._delete_job("drop")
            previous_drop = self._tasks.targets.get(identity, {})
            job["legacy_cleanup"] = bool(
                previous_drop.get("legacy_cleanup")
                or "manifest" not in self._tasks.namespaces[namespace]
            )
            job["published_artifacts"] = {
                str(index): path
                for index, path in enumerate(
                    p
                    for i, j in self._tasks.targets.items()
                    if i.namespace == namespace
                    for p in j.get("published_artifacts", {}).values()
                )
            }
            job["incarnations"] = list(
                dict.fromkeys(
                    [
                        *previous_drop.get("incarnations", []),
                        *previous_drop.get("retired_incarnations", []),
                        self._tasks.namespaces[namespace]["incarnation"],
                    ]
                )
            )
            operation_id = uuid.uuid4().hex
            for (identity_running, _), cancellation in self._tasks.cancellations.items():
                if identity_running.namespace == namespace:
                    cancellation._cancel("drop")
            with self._catalog.transaction():
                self._catalog.delete_namespace(namespace)
                self._catalog.delete_targets(namespace)
                self._catalog.put_target(namespace, "", job)
                self._catalog.add_wait_operation(operation_id, [str(job["revision"])])
            self._tasks.namespaces.pop(namespace)
            self._bindings.pop(namespace, None)
            self._index_errors.discard(namespace)
            self._tasks.visible = {
                i: v for i, v in self._tasks.visible.items() if i.namespace != namespace
            }
            self._tasks.targets = {
                i: j for i, j in self._tasks.targets.items() if i.namespace != namespace
            }
            self._tasks.refresh_pending()
            self._tasks.remember(identity, job)
            return DropReport(namespace, True, False, operation_id)

    def status(self) -> Status:
        with self._call(activity=False), self._condition:
            ready = self._is_ready() and not self._index_errors
            enabled = [ns for ns in self._tasks.namespaces if self._dense_config(ns) is not None]
            return Status(
                self._catalog.namespace_count(),
                self._catalog.document_count(),
                "dirty" if self._index_errors else "ready" if ready else "pending",
                bool(enabled),
                any(ns in self._bindings for ns in enabled),
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
        with self._condition:
            if self._stopping:
                raise Closed("MFS is closing")
            ns = self._tasks.namespaces[identity.namespace]
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
            previous = self._tasks.targets.get(identity)
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
                    not self._tasks.pending and self._state == "ready",
                    previous["revision"],
                    operation_id,
                )
                with self._catalog.transaction():
                    self._catalog.add_wait_operation(operation_id, [str(previous["revision"])])
                    if idempotency_key is not None:
                        self._catalog.put_operation(idempotency_key, request_hash, asdict(report))
                return report
            revision = uuid.uuid4().hex
            external = ns["kind"] == "external"
            object_name = (
                str(staged.path)
                if external
                else (self._artifacts.directory(ns["incarnation"], "originals") / revision)
                .relative_to(self._path)
                .as_posix()
            )
            if not external:
                os.replace(staged.path, self._path / object_name)
                self._fsync_directory((self._path / object_name).parent)
            source = dict(
                size=staged.size,
                mtime_ns=staged.mtime_ns if external else None,
                object=None if external else object_name,
                path=object_name if external else None,
            )
            job = dict(
                revision=revision,
                identity=asdict(identity),
                force=force,
                enqueued_at=time.time(),
                published_artifacts={},
                cleanup=previous is not None,
                borrowed_input=external,
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
                indexed_revision=None,
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
                    self._catalog.delete_document(identity.namespace, identity.doc_id)
                    self._catalog.put_target(identity.namespace, identity.doc_id, job)
                    self._catalog.add_wait_operation(operation_id, [revision])
                    if idempotency_key is not None:
                        self._catalog.put_operation(idempotency_key, request_hash, asdict(report))
            except Exception:
                # A lost ACK must not leave durable accepted work out of the live pending set.
                durable = self._catalog.get_target(identity.namespace, identity.doc_id)
                if durable is not None:
                    self._tasks.remember(identity, durable)
                raise
            self._tasks.visible.pop(identity, None)
            self._tasks.remember(identity, job)
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
            old = self._tasks.targets.get(identity)
            if old is None or old["kind"] != "upsert":
                operation_id = uuid.uuid4().hex
                with self._catalog.transaction():
                    self._catalog.add_wait_operation(
                        operation_id, [str(old["revision"])] if old else []
                    )
                return MutationReport(
                    identity,
                    "not_found",
                    not self._tasks.pending and self._state == "ready",
                    old["revision"] if old else None,
                    operation_id,
                )
            job = self._delete_job()
            job["incarnation"] = old.get("incarnation")
            job["indexed_revision"] = None
            job["published_artifacts"] = old.get("published_artifacts", {})
            operation_id = uuid.uuid4().hex
            with self._catalog.transaction():
                self._catalog.delete_document(identity.namespace, identity.doc_id)
                self._catalog.put_target(identity.namespace, identity.doc_id, job)
                self._catalog.add_wait_operation(operation_id, [str(job["revision"])])
            self._tasks.visible.pop(identity, None)
            self._tasks.remember(identity, job)
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
            previous = self._tasks.targets.get(document_id)
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
            if job.get("cleanup"):
                job["cleanup_restore_state"] = "pending"
            with self._catalog.transaction():
                self._catalog.set_cancelled(document_id.namespace, document_id.doc_id, False)
                self._catalog.put_target(document_id.namespace, document_id.doc_id, job)
            self._tasks.remember(document_id, job)

    def cancel(self, document_id: DocumentId) -> None:
        with self._call(), self._condition:
            previous = self._tasks.targets.get(document_id)
            if previous is None:
                raise InvalidQuery("document has no task")
            if previous["state"] == "succeeded":
                return
            if previous["kind"] in ("delete", "drop"):
                return
            job = copy.deepcopy(previous)
            job["state"] = "cancelled"
            job["attempt_token"] = uuid.uuid4().hex
            with self._catalog.transaction():
                self._catalog.set_cancelled(document_id.namespace, document_id.doc_id, True)
                self._catalog.put_target(document_id.namespace, document_id.doc_id, job)
            self._tasks.remember(document_id, job)

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
                return MutationReport(
                    document_id, "updated", False, job["revision"], report.operation_id
                )
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

    def _worker(self, role: str) -> None:
        del role
        preferred: DocumentId | None = None
        while True:
            with self._condition:
                if self._stopping:
                    return
                now = time.time()
                chosen: tuple[DocumentId, dict[str, Any]] | None = None
                next_run: float | None = None
                candidates = sorted(
                    ((i, self._tasks.targets[i]) for i in self._tasks.pending),
                    key=lambda pair: (
                        not bool(
                            pair[1].get("cleanup")
                            or pair[1]["kind"] in ("delete", "drop", "rebuild")
                        ),
                        pair[0] != preferred,
                        self._priority(*pair),
                    ),
                )
                for identity, item in candidates:
                    if item["state"] not in ("pending", "retry_wait") and not (
                        item.get("cleanup") and item["state"] == "cancelled"
                    ):
                        continue
                    if item["kind"] == "upsert" and not item.get("cleanup"):
                        if identity.namespace not in self._bindings:
                            continue
                        ns = self._tasks.namespaces[identity.namespace]
                        if item["stage"] != "process" and ns["paused"] and ns["indexing"] != "off":
                            continue
                        if identity.namespace in self._index_errors and item["stage"] != "process":
                            continue
                    due = float(item.get("next_run", 0))
                    if due > now:
                        next_run = due if next_run is None else min(next_run, due)
                        continue
                    chosen = identity, copy.deepcopy(item)
                    break
                if chosen is None:
                    preferred = None
                    self._condition.wait(None if next_run is None else max(0.01, next_run - now))
                    continue
                identity, job = chosen
                preferred = identity
                if job.get("cleanup"):
                    job.setdefault(
                        "cleanup_restore_state",
                        "cancelled" if job["state"] == "cancelled" else "pending",
                    )
                job.update(
                    state="cancelled"
                    if job.get("cleanup_restore_state") == "cancelled"
                    else "running",
                    attempts=int(job.get("attempts", 0)) + 1,
                    attempt_token=uuid.uuid4().hex,
                )
                job.setdefault("identity", asdict(identity))
                try:
                    self._tasks.persist(identity, job)
                except Exception:
                    self._condition.wait(0.25)
                    continue
                execution = (identity, str(job["attempt_token"]))
                self._tasks.executing.add(execution)
                self._tasks.cancellations[execution] = Cancellation()
                self._last_activity = time.monotonic()
            try:
                if job.get("cleanup"):
                    # One writer: a newer generation cannot publish before this call retires.
                    self._namespace_index(
                        identity.namespace, job.get("incarnation")
                    ).delete_document(identity, incarnation=job.get("incarnation"))
                    self._namespace_index(identity.namespace, job.get("incarnation")).flush()
                    with self._condition:
                        if self._tasks.current(identity, job):
                            job.update(
                                cleanup=False, state=job.pop("cleanup_restore_state", "pending")
                            )
                            self._tasks.persist(identity, job)
                elif job["stage"] == "process":
                    self._process_job(identity, job)
                else:
                    self._index_job(identity, job)
            except (_ProcessingYielded, _ProcessingStopped):
                self._tasks.advance(identity, job)
            except Exception as error:
                self._fail_job(identity, job, error)
            finally:
                with self._condition:
                    self._tasks.executing.discard(execution)
                    self._tasks.cancellations.pop(execution, None)
                    self._last_activity = time.monotonic()
                    self._condition.notify_all()

    def _fail_job(self, identity: DocumentId, job: dict[str, Any], error: Exception) -> None:
        with self._condition:
            if not self._tasks.current(identity, job):
                return
            # Handlers may have changed their local stage before a transaction rolled back.
            # Resume the durable stage, or adopt a transaction that committed before raising.
            previous = self._tasks.targets[identity]
            try:
                durable = self._catalog.get_target(identity.namespace, identity.doc_id)
            except Exception:
                durable = None
            if durable is not None and durable != previous:
                self._tasks.remember(identity, durable)
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
                self._tasks.persist(identity, job)
            except Exception as persistence_error:
                # Keep failed work pending even when the completion/error transaction itself fails.
                job.update(
                    state="retry_wait",
                    next_run=time.time() + 0.5,
                    error=f"{error}; state persistence failed: {persistence_error}",
                )
                self._tasks.remember(identity, job)

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
        artifact = self._write_artifact(job["revision"] + "-snapshot", record, job["incarnation"])
        return artifact, record

    def _process_job(self, identity: DocumentId, job: dict[str, Any]) -> None:
        _artifact, record = self._prepare_snapshot(job)
        self._tasks.processed(identity, job, record)

    def _index_job(self, identity: DocumentId, job: dict[str, Any]) -> None:
        from ._indexing import execute

        execute(self, identity, job)

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
            old = self._bindings.get(namespace)
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
            len(self._namespace_index(namespace).scan()),
            self._dense_config(namespace) is not None,
        )

    def _request_rebuild(
        self, namespace: str, binding: NamespaceBinding, changes: dict[str, Any] | None = None
    ) -> None:
        with self._condition:
            record = dict(self._tasks.namespaces[namespace], **(changes or {}))
            record["pending_manifest"] = binding.manifest
            previous = self._tasks.targets.get(DocumentId(namespace, ""), {})
            job = dict(
                self._delete_job("rebuild"),
                identity=asdict(DocumentId(namespace, "")),
                incarnation=record["incarnation"],
                manifest=binding.manifest,
                legacy_cleanup=bool(previous.get("legacy_cleanup")),
                retired_incarnations=list(
                    dict.fromkeys(
                        [
                            *previous.get("retired_incarnations", []),
                            *previous.get("incarnations", []),
                        ]
                    )
                ),
            )
            with self._catalog.transaction():
                self._catalog.put_namespace(namespace, record)
                self._catalog.put_target(namespace, "", job)
            self._tasks.namespaces[namespace] = record
            self._bindings[namespace] = binding
            self._tasks.visible = {
                i: v for i, v in self._tasks.visible.items() if i.namespace != namespace
            }
            self._tasks.remember(DocumentId(namespace, ""), job)

    def _rebuild_job(self, identity: DocumentId, job: dict[str, Any]) -> None:
        for incarnation in job.get("retired_incarnations", []):
            if incarnation != job["incarnation"]:
                retired = self._namespace_index(identity.namespace, incarnation)
                if retired.client.has_collection(retired.collection_name):
                    retired.drop()
        dense = job["manifest"]["index"]["dense"]
        index = self._namespace_index(identity.namespace, job["incarnation"])
        index.recreate(dense_dimension=int(dense["dimension"]) if dense else None)
        if job.get("legacy_cleanup") and self._index.client.has_collection(
            self._index.collection_name
        ):
            self._index.delete_namespace(identity.namespace)
        with self._condition:
            if not self._tasks.current(identity, job):
                return
            record = dict(
                self._tasks.namespaces[identity.namespace],
                manifest=job["manifest"],
                legacy_collection=False,
            )
            record.pop("pending_manifest", None)
            updates: list[tuple[DocumentId, dict[str, Any]]] = []
            with self._catalog.transaction():
                self._catalog.put_namespace(identity.namespace, record)
                for target_id, previous in self._tasks.targets.items():
                    if target_id.namespace != identity.namespace or target_id == identity:
                        continue
                    if previous["kind"] != "upsert":
                        target = dict(previous, state="succeeded", indexed_revision=None)
                    elif previous["stage"] == "process" or previous["state"] == "cancelled":
                        continue
                    else:
                        target = dict(
                            previous,
                            stage="chunk",
                            state="pending",
                            vectors=[],
                            indexed_revision=None,
                            error=None,
                            failures=0,
                            next_run=0,
                        )
                    self._catalog.put_target(target_id.namespace, target_id.doc_id, target)
                    updates.append((target_id, target))
                job.update(state="succeeded")
                self._catalog.put_target(identity.namespace, identity.doc_id, job)
            self._tasks.namespaces[identity.namespace] = record
            self._index_errors.discard(identity.namespace)
            for target_id, target in updates:
                self._tasks.remember(target_id, target)
            self._tasks.remember(identity, job)

    def _write_artifact(self, name: str, value: Any, incarnation: str) -> str:
        revision = name.removesuffix("-snapshot") if name.endswith("-snapshot") else None
        relative = (
            (
                self._artifacts.directory(incarnation, "derived")
                / (name + "-" + uuid.uuid4().hex + ".json")
            )
            .relative_to(self._path)
            .as_posix()
        )
        with self._catalog.transaction():
            self._catalog.register_artifact(relative)
        self._write_json(self._path / relative, value)
        if revision is not None:
            with self._condition, self._catalog.transaction():
                identity = value.get("identity")
                current = self._tasks.targets.get(DocumentId(**identity)) if identity else None
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
        path = self._artifacts.path(relative)
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
            for identity, job in self._tasks.targets.items():
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
                path = self._artifacts.path(name)
                if not path.is_file():
                    raise CorruptState(f"missing or unsafe managed artifact {name!r}")

    def grep(
        self,
        filters: Sequence[Filter] = (),
        select: Select = "doc_id",
        limit: int | None = 100,
        *,
        budget: GrepBudget | None = None,
    ) -> GrepResult[Any]:
        from ._reader import grep

        with self._call():
            return grep(self, filters, select, limit, budget or GrepBudget())

    def read(self, document_id: DocumentId) -> Document | None:
        with self._call():
            with self._condition:
                self._require_modern_namespace(document_id.namespace)
                if self._excluded(document_id.namespace, document_id.doc_id):
                    return None
                record = self._catalog.get_document(document_id.namespace, document_id.doc_id)
            return self._document(document_id, record) if record else None

    @staticmethod
    def _text_matches(
        text: str, text_filter: TextMatch, *, limit: int | None = None
    ) -> list[tuple[int, int]]:
        pattern = text_filter.pattern
        if not isinstance(_runtime(pattern), str) or not pattern or len(pattern.encode()) > 16384:
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
            raise InvalidQuery("grep limit must be 1..100000 or None")

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
        from ._reader import search

        with self._call():
            return search(self, text, filters, mode, select, limit, consistency, timeout)

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
        with self._call(), self._mutation_lock, self._condition:
            self._required_namespace(namespace)
            previous = self._tasks.namespaces[namespace]
            if previous["rules_revision"] != expected_revision:
                raise RuleConflict("namespace rules changed; read rules and retry")
            rules = {r["rule_id"]: IgnoreRule(**r) for r in previous["rules"]}
            for rule_id in remove:
                if rule_id not in rules:
                    raise InvalidConfiguration(f"unknown rule_id {rule_id!r}")
                del rules[rule_id]
            for rule in replace:
                if rule.rule_id not in rules:
                    raise InvalidConfiguration(f"unknown rule_id {rule.rule_id!r}")
                rules[rule.rule_id] = rule
            for rule in add:
                if rule.rule_id in rules:
                    raise InvalidConfiguration(f"duplicate rule_id {rule.rule_id!r}")
                rules[rule.rule_id] = rule
            if order is not None:
                if len(order) != len(rules) or set(order) != set(rules):
                    raise InvalidConfiguration("order must contain every rule_id exactly once")
                rules = {rule_id: rules[rule_id] for rule_id in order}
            ordered = validate_rules(tuple(rules.values()))
            record = dict(
                previous, rules=[asdict(r) for r in ordered], rules_revision=uuid.uuid4().hex
            )
            updates: list[tuple[DocumentId, dict[str, Any]]] = []
            with self._catalog.transaction():
                self._catalog.put_namespace(namespace, record)
                for identity, target in self._tasks.targets.items():
                    if (
                        identity.namespace != namespace
                        or target["kind"] != "upsert"
                        or not excluded(ordered, identity.doc_id)
                    ):
                        continue
                    deletion = dict(
                        self._delete_job(),
                        incarnation=target["incarnation"],
                        identity=asdict(identity),
                    )
                    self._catalog.delete_document(namespace, identity.doc_id)
                    self._catalog.put_target(namespace, identity.doc_id, deletion)
                    updates.append((identity, deletion))
            self._tasks.namespaces[namespace] = record
            for identity, deletion in updates:
                self._tasks.remember(identity, deletion)
            self._condition.notify_all()
            return RuleSet(record["rules_revision"], ordered)

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
                binding = self._binding(namespace)
                desired = NamespaceBinding.build(
                    binding.processors, binding.chunker, binding.embedder, mode
                )
                self._request_rebuild(namespace, desired, changes)
            else:
                record = dict(previous, **changes)
                with self._catalog.transaction():
                    self._catalog.put_namespace(namespace, record)
                self._tasks.namespaces[namespace] = record
            self._condition.notify_all()

    def reprocess_namespace(
        self, namespace: str, *, processors: Sequence[Processor]
    ) -> SyncReport | tuple[MutationReport, ...]:
        with self._call(), self._mutation_lock:
            binding = self._binding(namespace)
            previous = self._tasks.namespaces[namespace]
            if "pending_manifest" in previous:
                raise IndexUnavailable(
                    "finish the pending index rebuild before changing Processors"
                )
            desired = NamespaceBinding.build(
                processors, binding.chunker, binding.embedder, previous["indexing"]
            )
            with self._condition:
                record = dict(previous, manifest=desired.manifest, binding=uuid.uuid4().hex)
                updates: list[tuple[DocumentId, dict[str, Any]]] = []
                reports: list[MutationReport] = []
                for identity, old in self._tasks.targets.items():
                    if identity.namespace != namespace or old["kind"] != "upsert":
                        continue
                    route = next(
                        (
                            p
                            for p in desired.processors
                            if old["media_type"] in desired.media_types[id(p)]
                        ),
                        None,
                    )
                    job = {
                        k: copy.deepcopy(old[k])
                        for k in (
                            "identity",
                            "input",
                            "borrowed_input",
                            "source",
                            "incarnation",
                            "content_hash",
                            "media_type",
                        )
                        if k in old
                    }
                    job.update(
                        revision=uuid.uuid4().hex,
                        kind="upsert",
                        stage="process",
                        state="pending",
                        attempts=0,
                        failures=0,
                        next_run=0,
                        error=None,
                        processor=desired.descriptions[id(route)] if route else None,
                        binding=record["binding"],
                        indexed_revision=None,
                        cleanup=True,
                        enqueued_at=time.time(),
                        force=True,
                    )
                    updates.append((identity, job))
                    reports.append(
                        MutationReport(
                            identity, "updated", False, job["revision"], uuid.uuid4().hex
                        )
                    )
                with self._catalog.transaction():
                    self._catalog.put_namespace(namespace, record)
                    for (identity, job), report in zip(updates, reports, strict=True):
                        self._catalog.delete_document(namespace, identity.doc_id)
                        self._catalog.clear_prepared(self._tasks.targets[identity]["revision"])
                        self._catalog.set_cancelled(namespace, identity.doc_id, False)
                        self._catalog.put_target(namespace, identity.doc_id, job)
                        assert report.operation_id is not None
                        self._catalog.add_wait_operation(report.operation_id, [job["revision"]])
                self._tasks.namespaces[namespace] = record
                self._bindings[namespace] = desired
                for identity, job in updates:
                    self._tasks.remember(identity, job)
            if previous["kind"] == "external":
                return self._sync(namespace, ".", verify="content")
            return tuple(reports)

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
        binding = self._bindings.get(namespace)
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

    def _embed_documents(self, namespace: str, texts: Sequence[str]) -> list[list[float]]:
        embedder = self._matching_embedder(namespace)
        result: list[list[float]] = []
        for start in range(0, len(texts), 128):
            batch = texts[start : start + 128]
            try:
                result.extend(
                    self._validate_vectors(
                        embedder.embed_documents(batch), len(batch), embedder.dimension
                    )
                )
            except MFSError:
                raise
            except Exception as error:
                raise EmbeddingFailed(f"document embedding failed: {error}") from error
        return result

    def _embed_query(self, namespace: str, text: str) -> list[float]:
        embedder = self._matching_embedder(namespace)
        try:
            return self._validate_vectors([embedder.embed_query(text)], 1, embedder.dimension)[0]
        except MFSError:
            raise
        except Exception as error:
            raise EmbeddingFailed(f"query embedding failed: {error}") from error

    def _matching_embedder(self, namespace: str) -> Embedder:
        binding = self._binding(namespace)
        dense = self._dense_config(namespace)
        if dense is None or binding.embedder is None:
            raise CapabilityUnavailable(f"namespace {namespace!r} has no dense index")
        if (
            dense["embedding_space"] != binding.embedder.embedding_space
            or dense["dimension"] != binding.embedder.dimension
        ):
            raise NamespaceCompatibilityError("Embedder declaration changed after binding")
        return binding.embedder

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
        value = record["source"].get("object")
        return self._artifacts.path(value) if value is not None else None

    def _read_text(self, record: dict[str, Any], *, grep: bool = False) -> str:
        if record.get("transient") and not grep:
            if self._transient_text is None or self._transient_text[0] != record["revision"]:
                from ._preparation import prepare

                identity = DocumentId(**record["identity"])
                job = self._tasks.targets[identity]
                processor = self._processor_for(job)
                if processor is None:
                    raise CapabilityUnavailable("transient index text requires its Processor")
                prepare(self, identity, job, processor)
            assert self._transient_text is not None
            return self._transient_text[1]
        reference = record.get("grep_ref") if grep else None
        reference = reference or record.get("text_ref")
        if reference is None:
            if "text" in record:  # Legacy catalog migration, removed after schema upgrade.
                return str(record["text"])
            raise CorruptState("document has no text reference")
        path = (
            self._artifacts.path(reference["path"])
            if reference["owned"]
            else Path(reference["path"])
        )
        try:
            return path.read_text(encoding=reference.get("encoding", "utf-8"))
        except (OSError, UnicodeError) as error:
            raise SourceUnavailable(f"search text is unavailable: {path}: {error}") from error

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
            text = self._read_text(record, grep=True)
            reference = record.get("grep_ref") or record.get("text_ref")
            source_map = self._source_map(record)
            if (
                reference
                and not reference["owned"]
                and (blake3.blake3(text.encode()).hexdigest() != record.get("text_hash"))
            ):
                from ._reader import _line_map

                source_map = _line_map(text)
            return Document(
                id=document_id,
                snapshot_id=str(record["snapshot_id"]),
                media_type=str(record["media_type"]),
                text=text,
                source_map=source_map,
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
