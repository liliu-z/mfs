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
from ._cleanup import IndexCleanup
from ._configuration import Configuration
from ._indexing import Indexing
from ._json import JSONValue, copy_json, load_json
from ._lifecycle import Lifecycle, ReadView
from ._locks import CallGate
from ._namespace import NamespaceBinding
from ._platform import ProcessOwner, descriptor_change_time
from ._preparation import Preparation
from ._quiesce import ScopeLease
from ._reader import Reader
from ._rules import excluded, validate_rules
from ._runtime import NamespaceRuntime
from ._source import open_regular
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
    CorruptState,
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
    Superseded,
    UnsupportedMediaType,
    WaitTimeout,
    WrongNamespaceKind,
)
from .execution import Admission, ExecutionPolicy, ResourceLease
from .types import (
    Chunker,
    ConfigurationReport,
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
    original_revision: str | None = None
    namespace_incarnation: str | None = None


class MFS:
    @classmethod
    def open(
        cls,
        mfs_path: Path,
        *,
        gc_policy: GCPolicy | None = None,
        execution: ExecutionPolicy | None = None,
        admission: Admission | None = None,
        start_paused: bool = False,
    ) -> MFS:
        return cls(
            mfs_path,
            gc_policy=gc_policy,
            execution=execution,
            admission=admission,
            start_paused=start_paused,
        )

    def __init__(
        self,
        mfs_path: Path,
        *,
        gc_policy: GCPolicy | None = None,
        execution: ExecutionPolicy | None = None,
        admission: Admission | None = None,
        start_paused: bool = False,
    ) -> None:
        if not isinstance(_runtime(start_paused), bool):
            raise InvalidConfiguration("start_paused must be a boolean")
        policy = execution or ExecutionPolicy()
        self._path = Path(mfs_path).expanduser().resolve()
        self._condition = threading.Condition(threading.RLock())
        self._mutation_lock = threading.RLock()
        self._scan_locks: dict[str, threading.RLock] = {}
        self._stage_leases: dict[Path, list[ResourceLease]] = {}
        self._admission_sequences: dict[DocumentId, int] = {}
        self._admission_committed: dict[DocumentId, int] = {}
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
        self._close_lock = threading.Lock()
        self._close_done = threading.Event()
        self._close_error: BaseException | None = None
        self._stopping = False
        self._state: IndexState = "ready"
        self._workers: list[threading.Thread] = []
        from ._search_execution import SearchExecution

        self._search_execution = SearchExecution(policy.queries)
        self._grep_execution = SearchExecution(policy.queries)
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
            self._process_owner = ProcessOwner(self._path / "PROCESS_LOCK")
            for name in ("objects", "artifacts", "staging", "work", "namespaces"):
                (self._path / name).mkdir(exist_ok=True)
            self._catalog = Catalog(self._path / "catalog.sqlite", initialize=initialize)
            self._tasks = Lifecycle(
                self._catalog, self._condition, stage_timeout=policy.stage_timeout
            )
            self._tasks.boot_paused = start_paused
            self._artifacts = ArtifactStore(self._path, self._catalog, self._tasks, self._gc_policy)
            self._runtime = NamespaceRuntime(self._path, self._tasks, policy, admission)
            self._configuration = Configuration(self._tasks, self._runtime)
            self._index_cleanup = IndexCleanup(self._tasks, self._runtime)
            self._preparation = Preparation(
                self._path,
                self._catalog,
                self._artifacts,
                self._tasks,
                self._runtime,
                self._process_owner.descriptor,
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
            watchdog = threading.Thread(target=self._tasks.watch_deadlines, name="mfs-deadlines")
            watchdog.start()
            self._workers.append(watchdog)
            for number in range(policy.workers):
                worker = threading.Thread(target=self._worker.run, name=f"mfs-worker-{number}")
                worker.start()
                self._workers.append(worker)
            configurations = threading.Thread(
                target=self._maintain_configurations, name="mfs-configurations"
            )
            configurations.start()
            self._workers.append(configurations)
            if self._gc_policy.enabled:
                maintenance = threading.Thread(
                    target=self._artifacts.maintain, name="mfs-maintenance"
                )
                maintenance.start()
                self._workers.append(maintenance)
        except Exception:
            self._stopping = True
            self._search_execution.stop()
            self._grep_execution.stop()
            with self._condition:
                if hasattr(self, "_tasks"):
                    self._tasks.stop()
                self._condition.notify_all()
            for thread in self._workers:
                thread.join()
            self._search_execution.close()
            self._grep_execution.close()
            for name in ("_runtime", "_catalog", "_process_owner"):
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

    def _maintain_configurations(self) -> None:
        while True:
            with self._condition:
                if self._tasks.stopping:
                    return
                if self._tasks.boot_paused:
                    self._condition.wait(0.05)
                    continue
            try:
                self._index_cleanup.maintain()
                self._configuration.maintain()
            except StorageFailed as error:
                with self._condition:
                    self._tasks.storage_error = error
                    self._tasks.stop()
                return
            except Exception as error:
                with self._condition:
                    for namespace, record in list(self._tasks.namespaces.items()):
                        building = record.get("building")
                        if building and not building["initialized"]:
                            updated = dict(record, building=dict(building, error=str(error)))
                            try:
                                self._tasks.configure(namespace, updated)
                            except StorageFailed:
                                return
            with self._condition:
                self._condition.wait(0.05)

    def _is_ready(self) -> bool:
        return self._state == "ready" and self._tasks.is_ready()

    def resume_background(self) -> None:
        """Release the startup gate after the host has recovered its path journal."""
        with self._call(activity=False), self._condition:
            self._tasks.boot_paused = False
            self._condition.notify_all()

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

    def close(self, timeout: float | None = 30.0) -> None:
        """Stop admission and wait for actual retirement, at most timeout seconds.

        On WaitTimeout, cleanup continues and the store remains exclusively locked.
        A later close may wait again. Only the host can forcibly terminate a stuck
        in-process adapter by terminating the MFS process.
        """
        self._validate_timeout(timeout)
        started = time.monotonic()
        with self._close_lock:
            if self._calls.start_close():
                threading.Thread(target=self._close, name="mfs-close").start()
        remaining = None if timeout is None else max(0.0, timeout - (time.monotonic() - started))
        if not self._close_done.wait(remaining):
            raise WaitTimeout("MFS close timed out; cleanup continues with the store locked")
        if self._close_error is not None:
            raise self._close_error

    def _close(self) -> None:
        try:
            self._retire()
        except BaseException as error:
            self._close_error = error
        finally:
            self._calls.finish_close()
            self._close_done.set()

    def _retire(self) -> None:
        self._search_execution.stop()
        self._grep_execution.stop()
        self._tasks.stop()
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
        self._calls.drain()
        for thread in self._workers:
            thread.join()
        self._search_execution.close()
        self._grep_execution.close()
        try:
            self._runtime.close()
        finally:
            try:
                self._catalog.close()
            finally:
                try:
                    self._process_owner.close()
                finally:
                    self._instance_lock.release()

    def wait(
        self,
        target: DocumentId | str | MutationReport | DropReport | SyncReport | ConfigurationReport,
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
        elif isinstance(target, (DropReport, ConfigurationReport)):
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
                copy_json(
                    record.get("building", {}).get("manifest", record.get("pending_manifest"))
                ),
                record.get("max_file_bytes"),
                bool(record.get("processing_paused")),
                record["index_epoch"],
                record.get("building", {}).get("generation"),
                bool(record.get("retiring_generations")),
                record.get("retirement_error"),
                record.get("building", {}).get("error"),
                int(record.get("building", {}).get("failures", 0)),
                record.get("building", {}).get("next_run") or None,
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

    def quiesce(self, scopes: Sequence[UnderPath], timeout: float | None = None) -> ScopeLease:
        """Retire scoped executions/source reads until the returned lease is closed.

        This temporary gate does not alter user cancellation. Callers serialize
        their filesystem operations and sync observations while holding the lease.
        """
        self._validate_timeout(timeout)
        selected = tuple(scopes)
        if not selected:
            raise InvalidQuery("quiesce requires at least one explicit scope")
        with self._call(activity=False), self._condition:
            for scope in selected:
                self._required_namespace(scope.namespace)
                validate_external_path(scope.path, allow_root=True)
            token = self._tasks.quiesce(selected, timeout)
            return ScopeLease(selected, lambda: self._tasks.release_quiescence(token))

    def configure_processing(self, namespace: str, *, paused: bool) -> None:
        """Persistently pause preparation/new indexing; cleanup remains runnable."""
        if not isinstance(_runtime(paused), bool):
            raise InvalidConfiguration("paused must be a boolean")
        with self._call(), self._mutation_lock:
            self._tasks.configure_processing(namespace, paused)

    def restore_document_state(
        self,
        document_id: DocumentId,
        *,
        expected_revision: str,
        state: Literal["failed", "cancelled"],
        error: TaskError | None = None,
    ) -> None:
        """Import an unattempted target's stopped state while processing is paused.

        Replays are idempotent and do not undo later explicit retries. Source
        revisions, active attempts and the persisted namespace gate are checked.
        """
        if state not in ("failed", "cancelled") or (state == "failed" and error is None):
            raise InvalidQuery("import failed with TaskError, or cancelled")
        with self._call(), self._mutation_lock:
            self._tasks.restore_document_state(document_id, expected_revision, state, error)

    def open_artifact(self, document_id: DocumentId, name: str) -> ArtifactHandle:
        lease = contextlib.ExitStack()
        lease.enter_context(self._call())
        try:
            with self._condition:
                row = self._catalog.one(
                    "SELECT json_extract(value,'$.snapshot_id'),json_extract(value,'$.artifacts') "
                    "FROM documents WHERE namespace=? AND doc_id=?",
                    (document_id.namespace, document_id.doc_id),
                )
                artifacts = cast(dict[str, str], load_json(row[1])) if row and row[1] else {}
                if name not in artifacts:
                    raise InvalidQuery("document has no artifact with this name")
                lease.callback(self._artifacts.pin(artifacts[name]).release)
                path = self._artifacts.path(artifacts[name])
            assert row is not None
            return ArtifactHandle(path.open("rb"), str(row[0]), lease.close)
        except BaseException:
            lease.close()
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
        processing_paused: bool = False,
    ) -> NamespaceInfo:
        with self._call(), self._mutation_lock:
            validate_namespace(namespace)
            if kind not in ("internal", "external"):
                raise InvalidConfiguration("namespace kind must be internal or external")
            if not isinstance(_runtime(processing_paused), bool):
                raise InvalidConfiguration("processing_paused must be a boolean")
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
                    processing_paused=processing_paused,
                    rules=[asdict(r) for r in rules],
                    rules_revision=uuid.uuid4().hex,
                    max_file_bytes=policy.max_file_bytes,
                )
            index = self._runtime.index(namespace, record["incarnation"])
            dense = binding.manifest["index"]["dense"]
            index.recreate(dense_dimension=dense["dimension"] if dense else None)
            with self._condition:
                try:
                    self._tasks.configure(namespace, record)
                finally:
                    self._runtime.bind_if_current(namespace, binding)
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
        configuration_revision: str | None = None,
    ) -> NamespaceInfo:
        with self._call(), self._mutation_lock:
            self._required_namespace(namespace)
            self._require_modern_namespace(namespace)
            record = self._tasks.namespaces[namespace]
            building = record.get("building")
            if (
                configuration_revision is not None
                and building
                and (configuration_revision == building["generation"])
            ):
                binding = NamespaceBinding.build(
                    processors, chunker, embedder, building["indexing"]
                )
                binding.verify(namespace, building["manifest"])
                with self._condition:
                    current = self._tasks.namespaces.get(namespace, {})
                    if (
                        current.get("incarnation") != record["incarnation"]
                        or current.get("building", {}).get("generation") != configuration_revision
                    ):
                        raise NamespaceCompatibilityError("configuration changed during binding")
                    self._runtime.build_bindings[(namespace, configuration_revision)] = binding
                    for identity, job in list(self._tasks.build_targets.items()):
                        if identity.namespace == namespace and job["state"] == "blocked":
                            self._tasks.persist(identity, dict(job, state="pending", next_run=0))
                    self._condition.notify_all()
                return self._namespace_info(namespace, record)
            if (
                configuration_revision is not None
                and configuration_revision != record["index_epoch"]
            ):
                raise NamespaceCompatibilityError("configuration revision is no longer current")
            binding = NamespaceBinding.build(processors, chunker, embedder, record["indexing"])
            binding.verify(namespace, record.get("pending_manifest", record["manifest"]))
            if namespace in self._runtime.index_errors and "pending_manifest" not in record:
                raise IndexUnavailable(
                    f"{namespace}: collection is missing or incompatible; explicit reindex required"
                )
            with self._condition:
                current = self._tasks.namespaces.get(namespace, {})
                if (
                    current.get("incarnation") != record["incarnation"]
                    or current.get("index_epoch") != record["index_epoch"]
                    or current.get("pending_manifest") != record.get("pending_manifest")
                ):
                    raise NamespaceCompatibilityError("configuration changed during binding")
                self._runtime.bindings[namespace] = binding
                self._tasks.resume_blocked(namespace)
                self._condition.notify_all()
            return self._namespace_info(namespace, record)

    def configure_namespace(
        self,
        namespace: str,
        *,
        processors: Sequence[Processor] | None = None,
        chunker: Chunker | None = None,
        embedder: Embedder | None = None,
        indexing: IndexingMode | None = None,
    ) -> ConfigurationReport:
        """Durably accept one desired configuration; wait(report) follows its completion."""
        with self._call(), self._mutation_lock:
            self._required_namespace(namespace)
            record = self._tasks.namespaces[namespace]
            building = record.get("building")
            old = (
                self._runtime.build_bindings.get((namespace, building["generation"]))
                if building
                else self._runtime.bindings.get(namespace)
            )
            if old is None and processors is None:
                raise CapabilityUnavailable("configuration requires bound adapters")
            mode = indexing or (building or record)["indexing"]
            binding = NamespaceBinding.build(
                processors if processors is not None else old.processors if old else (),
                chunker or (old.chunker if old else None),
                embedder or (old.embedder if old else None),
                mode,
            )
            return self._configuration.request(namespace, binding, mode)

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
            try:
                return self._tasks.drop_namespace(namespace)
            finally:
                if namespace not in self._tasks.namespaces:
                    self._runtime.bindings.pop(namespace, None)
                    for key in list(self._runtime.build_bindings):
                        if key[0] == namespace:
                            self._runtime.build_bindings.pop(key)
                    self._runtime.index_errors.discard(namespace)

    def status(self) -> Status:
        with self._call(activity=False), self._condition:
            ready = self._is_ready() and not self._runtime.index_errors
            enabled = [
                ns for ns in self._tasks.namespaces if self._runtime.dense_config(ns) is not None
            ]
            return Status(
                self._catalog.namespace_count(),
                self._catalog.document_count(),
                "dirty"
                if self._runtime.index_errors or self._tasks.storage_error
                else "ready"
                if ready
                else "pending",
                bool(enabled),
                any(ns in self._runtime.bindings for ns in enabled),
                ready,
                len(self._tasks.pending)
                + sum(j["state"] != "succeeded" for j in self._tasks.build_targets.values()),
                sum(
                    j["state"] in ("failed", "blocked")
                    for j in (*self._tasks.targets.values(), *self._tasks.build_targets.values())
                ),
            )

    def document_status(self, document_id: DocumentId) -> DocumentStatus | None:
        with self._call(activity=False), self._condition:
            job = self._tasks.document_target(document_id)
            if job is None:
                return None
            text_revision = self._catalog.get_document_revision(
                document_id.namespace,
                document_id.doc_id,
                candidate=bool(job.get("build_generation")),
            )
            progress = self._tasks.progress.get(document_id, job.get("progress"))
            error = (
                TaskError(
                    job.get("error_code", "TaskFailed"), job["error"], bool(job.get("retryable"))
                )
                if job.get("error")
                else TaskError("StorageFailed", str(self._tasks.storage_error), True)
                if self._tasks.storage_error is not None and job["state"] != "succeeded"
                else None
            )
            return DocumentStatus(
                document_id,
                str(job["revision"]),
                text_revision,
                job.get("indexed_revision"),
                cast(TaskStage, job["stage"]),
                cast(TaskState, job["state"]),
                int(job["attempts"]),
                error.message if error else None,
                job.get("next_run") or None,
                int(job.get("completed_batches", 0)),
                int(job.get("batches", 0)),
                any(identity == document_id for identity, _ in self._tasks.executing),
                job.get("content_hash"),
                job.get("media_type"),
                job.get("source", {}).get("size"),
                job.get("source", {}).get("mtime_ns"),
                error,
                Progress(float(progress["completed"]), progress.get("total"), progress.get("unit"))
                if progress
                else None,
                tuple(job.get("artifacts", {})),
                self._tasks.active.get(document_id, {}).get("active_run_id"),
                self._tasks.active.get(document_id, {}).get("attempt_token"),
                self._catalog.cleanup_pending(document_id.namespace, document_id.doc_id),
                job.get("build_generation")
                or self._tasks.namespaces.get(document_id.namespace, {}).get("index_epoch"),
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
        with self._call():
            validate_internal_id(doc_id)
            identity = DocumentId(namespace, doc_id)
            with self._condition:
                if self._required_namespace(namespace).kind != "internal":
                    raise WrongNamespaceKind("upsert requires an internal namespace")
                if self._excluded(namespace, doc_id):
                    raise SourceExcluded(f"{namespace}:{doc_id} is excluded")
                incarnation = self._tasks.namespaces[namespace]["incarnation"]
                sequence = self._admission_sequences.get(identity, 0) + 1
                self._admission_sequences[identity] = sequence
            staged = (
                self._stage_bytes(data, namespace)
                if isinstance(data, bytes)
                else self._stage_path(Path(data), namespace)
            )
            try:
                self._prepare_original(staged, incarnation)
                selection = self._select_processor(
                    namespace,
                    doc_id,
                    staged.path,
                    media_type,
                    data if isinstance(data, Path) else None,
                )
                with self._condition:
                    if sequence < self._admission_committed.get(identity, 0):
                        raise Superseded("a newer request was accepted while copying this input")
                    try:
                        report = self._admit(
                            identity,
                            staged,
                            selection=selection,
                            idempotency_key=idempotency_key,
                        )
                    finally:
                        if (
                            self._tasks.targets.get(identity, {}).get("revision")
                            == staged.original_revision
                        ):
                            self._admission_committed[identity] = sequence
                    if self._tasks.targets.get(identity, {}).get("revision") == report.revision:
                        self._admission_committed[identity] = sequence
                    return report
            finally:
                self._remove_staging(staged.directory)

    def _admit(
        self,
        identity: DocumentId,
        staged: _Staged,
        *,
        selection: tuple[str, dict[str, Any]],
        idempotency_key: str | None = None,
        force: bool = False,
    ) -> MutationReport:
        media, description = self._select_processor(
            identity.namespace, identity.doc_id, staged.path, selection[0], None
        )
        if (media, description) != selection:
            raise SourceChanged("Processor configuration changed during input selection")
        if idempotency_key is not None and (
            not isinstance(_runtime(idempotency_key), str)
            or not idempotency_key
            or len(idempotency_key.encode()) > 2048
        ):
            raise InvalidQuery("idempotency_key must be a non-empty string of at most 2048 bytes")
        return self._tasks.accept(
            identity,
            SourceInput(
                staged.path,
                staged.content_hash,
                staged.size,
                staged.mtime_ns,
                media,
                description,
                staged.original_revision,
                staged.namespace_incarnation,
            ),
            self._artifacts,
            idempotency_key=idempotency_key,
            force=force,
        )

    def remove(self, namespace: str, doc_id: str) -> MutationReport:
        with self._call(), self._mutation_lock, self._condition:
            validate_internal_id(doc_id)
            if self._required_namespace(namespace).kind != "internal":
                raise WrongNamespaceKind(
                    "remove requires an internal namespace; use sync for external files"
                )
            identity = DocumentId(namespace, doc_id)
            try:
                return self._tasks.remove(identity)
            finally:
                if self._tasks.targets.get(identity, {}).get("kind") != "upsert":
                    sequence = self._admission_sequences.get(identity, 0) + 1
                    self._admission_sequences[identity] = sequence
                    self._admission_committed[identity] = sequence

    def retry(self, document_id: DocumentId, stage: TaskStage | None = None) -> None:
        with self._call():
            self._tasks.retry(document_id, stage)

    def cancel(self, document_id: DocumentId) -> None:
        with self._call():
            self._tasks.cancel(document_id)

    def reprocess(self, document_id: DocumentId) -> MutationReport:
        with self._call():
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
                staged = _Staged(
                    None,
                    self._path / str(input_name),
                    job["content_hash"],
                    job["source"]["size"],
                    job["source"].get("mtime_ns"),
                    uuid.uuid4().hex,
                    job["incarnation"],
                )
                sequence = self._admission_sequences.get(document_id, 0) + 1
                self._admission_sequences[document_id] = sequence
                try:
                    return self._admit(
                        document_id,
                        staged,
                        selection=self._select_processor(
                            document_id.namespace,
                            document_id.doc_id,
                            staged.path,
                            job["media_type"],
                            None,
                        ),
                        force=True,
                    )
                finally:
                    if (
                        self._tasks.targets.get(document_id, {}).get("revision")
                        == staged.original_revision
                    ):
                        self._admission_committed[document_id] = sequence

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
            accepted = self._configuration.request(
                namespace, binding, indexing or record["indexing"], force=True
            )
        self.wait(accepted, timeout)
        return ReindexReport(
            self._catalog.document_count(namespace),
            len(self._runtime.index(namespace).scan()),
            self._runtime.dense_config(namespace) is not None,
        )

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
            for (name,) in self._catalog.query("SELECT DISTINCT path FROM artifact_refs"):
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
        consistency: Consistency = "eventual",
        timeout: float | None = 5.0,
    ) -> GrepResult[Any]:
        from ._search_execution import SearchDeadline

        self._validate_timeout(timeout)
        selected_filters = tuple(filters)

        def execute(deadline: SearchDeadline) -> GrepResult[Any]:
            with self._call():
                return self._reader.grep(
                    namespace,
                    selected_filters,
                    select,
                    limit,
                    budget or GrepBudget(),
                    consistency,
                    deadline,
                )

        with self._calls.call():
            return self._grep_execution.run(timeout, execute)

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
        with self._call():
            return self._sync(namespace, path, verify=verify)

    def _sync(self, namespace: str, path: str, *, verify: str, force: bool = False) -> SyncReport:
        from ._sync import sync_namespace

        with self._condition:
            lock = self._scan_locks.setdefault(namespace, threading.RLock())
        with lock:
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
            building = previous.get("building")
            effective = building or previous
            mode = effective["indexing"] if indexing is None else indexing
            if mode != effective["indexing"]:
                binding = (
                    self._runtime.build_bindings.get((namespace, building["generation"]))
                    if building
                    else self._runtime.bindings.get(namespace)
                )
                if binding is None:
                    raise CapabilityUnavailable("configuration requires bound adapters")
                desired = NamespaceBinding.build(
                    binding.processors, binding.chunker, binding.embedder, mode
                )
                self._configuration.request(namespace, desired, mode)
            if paused is not None:
                record = dict(self._tasks.namespaces[namespace], paused=paused)
                self._tasks.configure(namespace, record)
            self._condition.notify_all()

    def reprocess_namespace(
        self, namespace: str, *, processors: Sequence[Processor]
    ) -> ConfigurationReport:
        """Compatibility entry point for a full, atomic processor rebuild."""
        with self._call(), self._mutation_lock, self._condition:
            binding = self._runtime.binding(namespace)
            previous = self._tasks.namespaces[namespace]
            desired = NamespaceBinding.build(
                processors, binding.chunker, binding.embedder, previous["indexing"]
            )
            return self._configuration.request(
                namespace, desired, previous["indexing"], force=True, force_process=True
            )

    def _stage_descriptor(self, descriptor: int, source: Path) -> _Staged:
        for _ in range(2):
            self._calls.check()
            before = os.fstat(descriptor)
            before_change = descriptor_change_time(descriptor)
            os.lseek(descriptor, 0, os.SEEK_SET)
            digest = blake3.blake3()
            while True:
                self._calls.check()
                block = os.read(descriptor, 1024 * 1024)
                if not block:
                    break
                digest.update(block)
            self._calls.check()
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
                    with (
                        open_regular(source.parent.resolve() / source.name) as input_stream,
                        staged_path.open("wb") as output_stream,
                    ):
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
            with self._condition:
                self._stage_leases[directory] = [
                    self._artifacts.pin(directory.relative_to(self._path).as_posix())
                ]
            directory.mkdir()
            return directory
        except OSError as error:
            self._remove_staging(directory)
            raise StorageFailed(f"failed to create staging directory: {error}") from error

    def _prepare_original(self, staged: _Staged, incarnation: str) -> None:
        revision = uuid.uuid4().hex
        destination = self._artifacts.directory(incarnation, "originals") / revision
        assert staged.directory is not None
        with self._condition:
            self._stage_leases[staged.directory].append(
                self._artifacts.pin(destination.relative_to(self._path).as_posix())
            )
        relative = self._artifacts.accept_original(staged.path, incarnation, revision)
        staged.path = self._path / relative
        staged.original_revision = revision
        staged.namespace_incarnation = incarnation

    def _remove_staging(self, directory: Path | None) -> None:
        if directory is None:
            return
        with contextlib.suppress(OSError):
            shutil.rmtree(directory)
        with self._condition:
            for lease in self._stage_leases.pop(directory, []):
                lease.release()

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
        with self._condition:
            self._require_modern_namespace(namespace)
            binding = self._runtime.bindings.get(namespace)
            record = self._tasks.namespaces[namespace]
            candidate_binding = self._runtime.build_bindings.get(
                (namespace, record.get("building", {}).get("generation"))
            )
        building = record.get("building", {})
        pending = building.get("manifest", {}).get("processors", [])
        descriptions = record["manifest"]["processors"]
        by_media = {
            media: {k: p[k] for k in ("id", "version", "options")}
            for p in [*pending, *descriptions]
            for media in p["media_types"]
        }
        by_suffix = {
            suffix: (media, by_media[media])
            for p in [*descriptions, *pending]
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
                with open_regular(staged_path) as stream:
                    head = stream.read(64 * 1024)
            binding = candidate_binding or binding
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
