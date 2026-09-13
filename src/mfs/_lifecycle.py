# pyright: reportPrivateUsage=false
from __future__ import annotations

import copy
import threading
import time
import uuid
from collections.abc import Callable, Container, Generator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Literal

import blake3

from ._catalog import Catalog
from ._json import JSONValue, canonical_json, compact_json
from ._namespace import NamespaceBinding
from ._rules import excluded, validate_rules
from ._validation import validate_namespace
from ._work import (
    Chunked,
    Cleaned,
    Embedded,
    ExecutionPermit,
    FileWork,
    NamespaceWork,
    NeedsPreparation,
    Prepared,
    Published,
    SourceInput,
    StepResult,
)
from .errors import (
    CapabilityUnavailable,
    Closed,
    ExecutionTimeout,
    IdempotencyConflict,
    InvalidConfiguration,
    InvalidQuery,
    MFSError,
    MigrationRequired,
    NamespaceNotFound,
    OperationFailed,
    ProcessingFailed,
    RetryableError,
    RuleConflict,
    StorageFailed,
    Superseded,
    WaitTimeout,
)
from .execution import ResourceLease
from .processing import Cancellation, _ProcessingStopped, _ProcessingYielded
from .types import (
    DocumentId,
    DropReport,
    IgnoreRule,
    MutationReport,
    RuleSet,
    TaskError,
    TaskStage,
    UnderPath,
)

if TYPE_CHECKING:
    from ._artifacts import ArtifactStore


class Lifecycle:
    """One owner for durable targets, live execution and publication eligibility.

    Callers hold condition for multi-record acceptance transactions. Slow work never
    holds it; its commit must validate the current revision and execution token.
    """

    def __init__(
        self, catalog: Catalog, condition: threading.Condition, *, stage_timeout: float = 300.0
    ) -> None:
        self.catalog, self.condition = catalog, condition
        self.stage_timeout = stage_timeout
        self.stopping = False
        self.boot_paused = False
        self.active_scopes: tuple[UnderPath, ...] = ()
        self.readers = 0
        self.last_activity = time.monotonic()
        self.targets: dict[DocumentId, dict[str, Any]] = {}
        self.build_targets: dict[DocumentId, dict[str, Any]] = {}
        self.target_changed: Callable[[DocumentId], None] | None = None
        self.state_reconciled: Callable[[], None] | None = None
        self.pin_artifact: Callable[[str], ResourceLease] | None = None
        self.pending: dict[DocumentId, str] = {}
        self.visible: dict[DocumentId, str] = {}
        self.progress: dict[DocumentId, dict[str, Any]] = {}
        self.executing: set[tuple[DocumentId, str]] = set()
        self.execution_records: dict[tuple[DocumentId, str], dict[str, Any]] = {}
        self.active: dict[DocumentId, dict[str, Any]] = {}
        self.cancellations: dict[tuple[DocumentId, str], Cancellation] = {}
        self.queries: dict[str, int] = {}
        self.generation_queries: dict[tuple[str, str | None], int] = {}
        self.namespace_executions: dict[str, int] = {}
        self.source_readers: dict[DocumentId, int] = {}
        self.quiescence: dict[str, tuple[tuple[UnderPath, str], ...]] = {}
        self.queued_at: dict[DocumentId, tuple[str, float]] = {}
        self.bound: Mapping[str, NamespaceBinding] = {}
        self.generation_bindings: Mapping[tuple[str, str], NamespaceBinding] = {}
        self.unavailable: Container[str] = ()
        self.storage_error: StorageFailed | None = None
        self.namespaces = dict(catalog.list_namespaces())
        with catalog.transaction():
            for ns, doc, encoded in catalog.query(
                "SELECT namespace,doc_id,value FROM build_targets"
            ):
                job = catalog.decode(encoded)
                if job["state"] in ("running", "blocked"):
                    job.update(state="pending", next_run=0)
                    catalog.put_build(ns, doc, job)
                self.build_targets[DocumentId(ns, doc)] = job
            for name, record in self.namespaces.items():
                record.setdefault("incarnation", uuid.uuid4().hex)
                record.setdefault("binding", uuid.uuid4().hex)
                record.setdefault("index_epoch", record["incarnation"])
                catalog.put_namespace(name, record)
            for ns, doc, job in catalog.list_targets():
                job.setdefault("identity", asdict(DocumentId(ns, doc)))
                # A legacy instance-wide rebuild is replaced by explicit namespace migrations.
                if not ns:
                    job.update(state="succeeded")
                    catalog.put_target(ns, doc, job)
                elif job["state"] in ("running", "blocked"):
                    job.update(state="pending", next_run=0)
                    catalog.put_target(ns, doc, job)
                self.targets[DocumentId(ns, doc)] = job
            for ns, doc, encoded in catalog.query("SELECT namespace,doc_id,value FROM active_runs"):
                identity = DocumentId(ns, doc)
                active = catalog.decode(encoded)
                target = self.work_target(identity, active)
                if (
                    target is None
                    or target["revision"] != active["revision"]
                    or target.get("build_generation") != active.get("build_generation")
                    or target.get("active_run_id") != active.get("active_run_id")
                ):
                    catalog.put_active(ns, doc, None)
                elif target["state"] not in ("succeeded", "cancelled", "failed", "blocked"):
                    active = dict(target, active_run_id=active["active_run_id"])
                    catalog.put_active(ns, doc, active)
                    self.active[identity] = active
                else:
                    catalog.put_active(ns, doc, None)
        self.refresh_pending()
        self.visible = {
            DocumentId(ns, doc): record["snapshot_id"]
            for ns, doc, record in catalog.list_documents()
            if "manifest" in self.namespaces.get(ns, {})
            and "pending_manifest" not in self.namespaces.get(ns, {})
            and self.targets.get(DocumentId(ns, doc), {}).get("indexed_revision")
            == record.get("revision")
        }

    def require_modern_namespace(self, namespace: str) -> None:
        validate_namespace(namespace)
        record = self.namespaces.get(namespace)
        if record is None:
            raise NamespaceNotFound(f"namespace {namespace!r} does not exist")
        if "manifest" not in record:
            raise MigrationRequired(
                f"{namespace}: call migrate_namespace with adapters and indexing mode"
            )

    def refresh_pending(self) -> None:
        self.pending = {
            identity: str(job["revision"])
            for identity, job in self.targets.items()
            if job["state"] != "succeeded"
        }

    def is_ready(self) -> bool:
        with self.condition:
            return (
                not self.stopping
                and not self.boot_paused
                and self.storage_error is None
                and not self.pending
                and not any(
                    n.get("building") or n.get("retiring_generations")
                    for n in self.namespaces.values()
                )
                and self.catalog.one("SELECT 1 FROM index_cleanup LIMIT 1") is None
            )

    def work_target(self, identity: DocumentId, job: dict[str, Any]) -> dict[str, Any] | None:
        return (self.build_targets if job.get("build_generation") else self.targets).get(identity)

    def load_work(self, identity: DocumentId, job: dict[str, Any]) -> dict[str, Any] | None:
        if job.get("build_generation"):
            return self.catalog.get_build(identity.namespace, identity.doc_id)
        return self.catalog.get_target(identity.namespace, identity.doc_id)

    def store_work(self, identity: DocumentId, job: dict[str, Any]) -> None:
        if job.get("build_generation"):
            self.catalog.put_build(identity.namespace, identity.doc_id, job)
        else:
            self.catalog.put_target(identity.namespace, identity.doc_id, job)

    @contextmanager
    def state_transaction(self) -> Generator[None]:
        """Reconcile an uncertain command before allowing any old permit to commit."""
        if self.storage_error is not None:
            raise self.storage_error
        try:
            with self.catalog.transaction():
                yield
        except BaseException:
            try:
                namespaces = dict(self.catalog.list_namespaces())
                targets = {DocumentId(ns, doc): job for ns, doc, job in self.catalog.list_targets()}
                documents = self.catalog.list_documents()
                for (identity, token), cancellation in self.cancellations.items():
                    target = targets.get(identity)
                    if target is None or target.get("attempt_token") != token:
                        cancellation._cancel("superseded")
                self.namespaces = namespaces
                if self.state_reconciled is not None:
                    self.state_reconciled()
                self.progress = {
                    i: p
                    for i, p in self.progress.items()
                    if targets.get(i, {}).get("revision") == self.targets.get(i, {}).get("revision")
                }
                self.targets = targets
                self.build_targets = {
                    DocumentId(ns, doc): self.catalog.decode(value)
                    for ns, doc, value in self.catalog.query(
                        "SELECT namespace,doc_id,value FROM build_targets"
                    )
                }
                self.refresh_pending()
                self.visible = {
                    DocumentId(ns, doc): record["snapshot_id"]
                    for ns, doc, record in documents
                    if "manifest" in namespaces.get(ns, {})
                    and "pending_manifest" not in namespaces.get(ns, {})
                    and targets.get(DocumentId(ns, doc), {}).get("indexed_revision")
                    == record.get("revision")
                }
                for identity, job in list(targets.items()):
                    self.remember(identity, job)
                self.condition.notify_all()
            except Exception as error:
                self.storage_error = StorageFailed(f"cannot reconcile state transaction: {error}")
                self.visible.clear()
                self.stop()
            raise

    def unfinished(
        self,
        namespaces: set[str] | None,
        identity: DocumentId | None = None,
        path: str = ".",
        *,
        readiness: bool = False,
    ) -> bool:
        """The shared completion/terminal-failure predicate for current and strong waits."""
        if self.storage_error is not None:
            raise self.storage_error
        if self.stopping:
            raise Closed("MFS instance is closing")
        pending = False
        if not readiness:
            for _, debt in self.catalog.cleanup_rows():
                if namespaces is not None and debt["namespace"] not in namespaces:
                    continue
                if identity is not None and (
                    debt["namespace"] != identity.namespace or debt["doc_id"] != identity.doc_id
                ):
                    continue
                if (
                    path != "."
                    and debt["doc_id"] != path
                    and not debt["doc_id"].startswith(path + "/")
                ):
                    continue
                if debt["failures"] >= 5:
                    raise OperationFailed(debt["error"] or "index cleanup failed", state="failed")
                pending = True
        for namespace, record in self.namespaces.items():
            if namespaces is not None and namespace not in namespaces:
                continue
            if not readiness and record.get("retiring_generations"):
                if record.get("retirement_failures", 0) >= 5:
                    raise OperationFailed(record["retirement_error"], state="failed")
                pending = True
            building = record.get("building")
            if building:
                pending = True
                if building.get("error") and building.get("failures", 0) >= 5:
                    raise OperationFailed(building["error"], state="failed")
                if (namespace, building["generation"]) not in self.generation_bindings:
                    raise OperationFailed(
                        "candidate configuration needs adapter binding", state="blocked"
                    )
        for current, job in [*self.targets.items(), *self.build_targets.items()]:
            if namespaces is not None and current.namespace not in namespaces:
                continue
            if (
                not job.get("build_generation")
                and job["kind"] == "upsert"
                and self.namespaces.get(current.namespace, {}).get("building")
            ):
                continue
            if current.doc_id and (
                (identity is not None and current != identity)
                or (
                    identity is None
                    and path != "."
                    and current.doc_id != path
                    and not current.doc_id.startswith(path + "/")
                )
            ):
                continue
            if readiness and job["kind"] in ("delete", "drop"):
                continue
            state = job["state"]
            if state in ("failed", "blocked", "cancelled"):
                raise OperationFailed(
                    job.get("error") or f"{current}: current target is {state}",
                    revision=job["revision"],
                    state=state,
                    error_code=job.get("error_code"),
                    retryable=bool(job.get("retryable")),
                )
            pending |= state != "succeeded"
        return pending

    def stop(self) -> None:
        with self.condition:
            self.stopping = True
            for cancellation in self.cancellations.values():
                cancellation._cancel("close")
            self.condition.notify_all()

    def watch_deadlines(self) -> None:
        with self.condition:
            while not self.stopping:
                try:
                    now = time.monotonic()
                    for execution, cancellation in tuple(self.cancellations.items()):
                        job = self.execution_records[execution]
                        if (
                            cancellation._deadline is not None
                            and now >= cancellation._deadline
                            and self.current(execution[0], job)
                        ):
                            error = ExecutionTimeout(
                                f"{job['stage']} exceeded {self.stage_timeout}s"
                            )
                            current = self.work_target(execution[0], job)
                            assert current is not None
                            self.persist(
                                execution[0],
                                dict(
                                    current,
                                    state="failed",
                                    error=str(error),
                                    error_code=error.code,
                                    retryable=False,
                                    next_run=0,
                                    attempt_token=uuid.uuid4().hex,
                                ),
                            )
                            cancellation._cancel("timeout")
                except Exception as error:
                    self.storage_error = StorageFailed(f"deadline could not persist: {error}")
                    self.stop()
                    return
                self.condition.wait(0.05)

    @staticmethod
    def in_scope(identity: DocumentId, scope: UnderPath) -> bool:
        return identity.namespace == scope.namespace and (
            not identity.doc_id
            or scope.path == "."
            or identity.doc_id == scope.path
            or identity.doc_id.startswith(scope.path + "/")
        )

    def held(self, identity: DocumentId, job: dict[str, Any]) -> bool:
        incarnations = {job.get("incarnation"), *job.get("incarnations", [])}
        return any(
            incarnation in incarnations and self.in_scope(identity, scope)
            for scopes in self.quiescence.values()
            for scope, incarnation in scopes
        )

    def quiesce(self, scopes: tuple[UnderPath, ...], timeout: float | None) -> str:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            token = uuid.uuid4().hex
            for scope in scopes:
                self.require_modern_namespace(scope.namespace)
            captured = tuple((s, self.namespaces[s.namespace]["incarnation"]) for s in scopes)
            self.quiescence[token] = captured
            for (identity, _), cancellation in self.cancellations.items():
                if any(self.in_scope(identity, s) for s in scopes):
                    cancellation._cancel("quiesce")
            self.condition.notify_all()
            try:
                while True:
                    if self.storage_error is not None:
                        raise self.storage_error
                    if self.stopping:
                        raise Closed("MFS instance is closing")
                    if not any(
                        self.namespace_executions.get(inc) for _, inc in captured
                    ) and not any(
                        self.in_scope(identity, scope)
                        for identity in (
                            *(i for i, _ in self.executing),
                            *self.source_readers,
                        )
                        for scope in scopes
                    ):
                        return token
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        raise WaitTimeout("scope executions have not retired")
                    self.condition.wait(remaining)
            except BaseException:
                self.release_quiescence(token)
                raise

    def release_quiescence(self, token: str) -> None:
        with self.condition:
            self.quiescence.pop(token, None)
            self.condition.notify_all()

    @contextmanager
    def source_read(self, identity: DocumentId) -> Generator[None]:
        with self.condition:
            record = self.namespaces.get(identity.namespace, {})
            if self.held(identity, record):
                raise CapabilityUnavailable("source scope is temporarily quiesced")
            self.source_readers[identity] = self.source_readers.get(identity, 0) + 1
        try:
            yield
        finally:
            with self.condition:
                remaining = self.source_readers[identity] - 1
                if remaining:
                    self.source_readers[identity] = remaining
                else:
                    self.source_readers.pop(identity)
                self.condition.notify_all()

    def configure_processing(self, namespace: str, paused: bool) -> None:
        with self.condition:
            self.require_modern_namespace(namespace)
            try:
                self.configure(
                    namespace, dict(self.namespaces[namespace], processing_paused=paused)
                )
            finally:
                if self.namespaces[namespace].get("processing_paused"):
                    for (identity, _), cancellation in self.cancellations.items():
                        if identity.namespace == namespace and identity.doc_id:
                            cancellation._cancel("pause")

    def restore_document_state(
        self,
        identity: DocumentId,
        expected_revision: str,
        state: Literal["failed", "cancelled"],
        error: TaskError | None,
    ) -> None:
        with self.condition:
            self.require_modern_namespace(identity.namespace)
            if not self.namespaces[identity.namespace].get("processing_paused"):
                raise InvalidQuery("state import requires processing_paused=True")
            previous = self.targets.get(identity)
            if (
                previous is None
                or previous["kind"] != "upsert"
                or previous["revision"] != expected_revision
            ):
                raise InvalidQuery("state import does not match the current source revision")
            imported = dict(state=state, error=asdict(error) if error else None)
            if previous.get("restored_state") == imported:
                return  # Replay must not undo a subsequent explicit retry/cancel.
            if (
                previous.get("restored_state")
                or previous["attempts"]
                or any(i == identity for i, _ in self.executing)
            ):
                raise InvalidQuery("state import only accepts an unattempted current target")
            if previous["state"] == "cancelled" and state != "cancelled":
                raise InvalidQuery("state import cannot clear a user cancellation")
            job = dict(
                previous,
                state=state,
                restored_state=imported,
                error=error.message if error else "imported user cancellation",
                error_code=error.code if error else "Cancelled",
                retryable=error.retryable if error else False,
                next_run=0,
                attempt_token=uuid.uuid4().hex,
            )
            with self.state_transaction():
                if state == "cancelled":
                    self.catalog.set_cancelled(identity.namespace, identity.doc_id, True)
                self.catalog.put_target(identity.namespace, identity.doc_id, job)
            self.remember(identity, job)

    def wait(
        self,
        namespace: str,
        identity: DocumentId | None,
        path: str,
        timeout: float | None,
    ) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            while self.unfinished({namespace}, identity, path):
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise WaitTimeout("current file/scope work has not completed")
                self.condition.wait(remaining)

    def remember(self, identity: DocumentId, job: dict[str, Any]) -> None:
        if job.get("build_generation"):
            self.build_targets[identity] = copy.deepcopy(job)
            active = self.active.get(identity, {})
            if active.get("build_generation") == job["build_generation"]:
                for (running_id, token), cancellation in self.cancellations.items():
                    if running_id == identity and (
                        token != job.get("attempt_token") or job["state"] == "cancelled"
                    ):
                        cancellation._cancel("superseded")
            self.condition.notify_all()
            return
        if (
            job["kind"] != "upsert"
            or job.get("indexed_revision") != job["revision"]
            or "pending_manifest" in self.namespaces.get(identity.namespace, {})
        ):
            self.visible.pop(identity, None)
        elif job.get("snapshot_id"):
            self.visible[identity] = job["snapshot_id"]
        if self.targets.get(identity, {}).get("revision") != job["revision"]:
            self.progress.pop(identity, None)
        for (running_id, token), cancellation in self.cancellations.items():
            if running_id == identity and (
                job.get("attempt_token") != token or job["state"] == "cancelled"
            ):
                cancellation._cancel("user" if job["state"] == "cancelled" else "superseded")
        self.targets[identity] = copy.deepcopy(job)
        if job["state"] == "succeeded":
            self.pending.pop(identity, None)
        else:
            self.pending[identity] = str(job["revision"])
        self.track_queue(identity, job)
        if self.target_changed is not None:
            self.target_changed(identity)
        self.condition.notify_all()

    def persist(self, identity: DocumentId, job: dict[str, Any]) -> None:
        try:
            with self.catalog.transaction():
                self.store_work(identity, job)
                if job.get("active_run_id"):
                    self.catalog.put_active(identity.namespace, identity.doc_id, job)
        except Exception:
            # A committed checkpoint/claim can lose its acknowledgement. Adopt the
            # exact durable value so a running target never becomes an orphan.
            if self.load_work(identity, job) != job:
                raise
        self.remember(identity, job)
        if job.get("active_run_id"):
            self.active[identity] = copy.deepcopy(job)

    def current(self, identity: DocumentId, job: dict[str, Any]) -> bool:
        current = self.work_target(identity, job)
        return (
            self.storage_error is None
            and current is not None
            and (identity, job.get("attempt_token", "")) in self.executing
            and current["revision"] == job["revision"]
            and current.get("active_run_id") == job.get("active_run_id")
            and current.get("build_generation") == job.get("build_generation")
            and (current["state"] != "cancelled" or bool(job.get("cleanup")))
            and current.get("attempt_token") == job.get("attempt_token")
            and (
                job["kind"] != "upsert"
                or job["stage"] == "process"
                or bool(job.get("cleanup"))
                or job.get("index_epoch")
                == (
                    self.namespaces.get(identity.namespace, {})
                    .get("building", {})
                    .get("generation")
                    if job.get("build_generation")
                    else self.namespaces.get(identity.namespace, {}).get("index_epoch")
                )
            )
            and (
                job["kind"] == "drop"
                or (
                    self.namespaces.get(identity.namespace, {}).get("incarnation")
                    == job.get("incarnation")
                    and (
                        job["kind"] != "upsert"
                        or not excluded(
                            tuple(
                                IgnoreRule(**r)
                                for r in self.namespaces[identity.namespace].get("rules", [])
                            ),
                            identity.doc_id,
                        )
                    )
                )
            )
        )

    def advance(self, identity: DocumentId, job: dict[str, Any], **changes: Any) -> None:
        with self.condition:
            if not self.current(identity, job):
                return
            target = self.work_target(identity, job)
            assert target is not None
            job = copy.deepcopy(target)
            job.update(state="pending", error=None, failures=0, next_run=0, **changes)
            self.persist(identity, job)

    def processed(self, identity: DocumentId, job: dict[str, Any], record: dict[str, Any]) -> None:
        with self.condition:
            if not self.current(identity, job):
                return
            target = dict(
                job,
                stage="chunk",
                state="pending",
                snapshot_id=record["snapshot_id"],
                text_ref=record.get("text_ref"),
                artifacts=record.get("artifacts", {}),
                checkpoint={},
                error=None,
                failures=0,
            )
            target.pop("refresh_text", None)
            with self.catalog.transaction():
                if job.get("build_generation"):
                    self.catalog.put_build(
                        identity.namespace, identity.doc_id, record, document=True
                    )
                else:
                    self.catalog.put_document(identity.namespace, identity.doc_id, record)
                self.store_work(identity, target)
                self.catalog.put_active(identity.namespace, identity.doc_id, target)
                self.catalog.clear_prepared(job["revision"])
            self.remember(identity, target)
            self.active[identity] = copy.deepcopy(target)

    def base_priority(self, identity: DocumentId, job: dict[str, Any]) -> int:
        return (
            0
            if job.get("force")
            else 1
            if any(
                scope.namespace == identity.namespace
                and (
                    scope.path == "."
                    or identity.doc_id == scope.path
                    or identity.doc_id.startswith(scope.path + "/")
                )
                for scope in self.active_scopes
            )
            else 2
        )

    def runnable(self, identity: DocumentId, job: dict[str, Any]) -> bool:
        if self.boot_paused:
            return False
        if self.held(identity, job):
            return False
        if any(i == identity for i, _ in self.executing):
            return False
        active = self.active.get(identity)
        if active and active.get("active_run_id") != job.get("active_run_id"):
            target = self.work_target(identity, active)
            if (
                target
                and target.get("revision") == active["revision"]
                and target.get("active_run_id") == active.get("active_run_id")
                and target.get("build_generation") == active.get("build_generation")
                and target["state"] in ("pending", "running", "retry_wait")
            ):
                return False
            with self.catalog.transaction():
                self.catalog.put_active(identity.namespace, identity.doc_id, None)
            self.active.pop(identity, None)
        if job["state"] not in ("pending", "retry_wait") and not (
            job.get("cleanup") and job["state"] == "cancelled"
        ):
            return False
        if job["kind"] in ("drop", "rebuild"):
            incarnations = [
                job.get("incarnation"),
                *job.get("incarnations", []),
                *job.get("retired_incarnations", []),
            ]
            if any(
                incarnation is not None
                and (
                    self.queries.get(incarnation, 0)
                    or self.namespace_executions.get(incarnation, 0)
                )
                for incarnation in incarnations
            ):
                return False
        if job["kind"] == "upsert" and not job.get("cleanup"):
            build = job.get("build_generation")
            namespace = self.namespaces[identity.namespace]
            if build and (
                (job["stage"] != "process" and not namespace.get("building", {}).get("initialized"))
                or (identity.namespace, build) not in self.generation_bindings
            ):
                return False
            if not build and identity.namespace not in self.bound:
                return False
            if namespace.get("processing_paused"):
                return False
            if job["stage"] != "process" and (
                "pending_manifest" in namespace
                or (namespace["paused"] and namespace["indexing"] != "off")
                or (not build and identity.namespace in self.unavailable)
            ):
                return False
        return True

    def candidates(self) -> tuple[list[tuple[DocumentId, dict[str, Any]]], float | None]:
        now, due_at = time.time(), None
        ready: list[tuple[DocumentId, dict[str, Any]]] = []
        for identity, job in [
            *((identity, self.targets[identity]) for identity in self.pending),
            *self.build_targets.items(),
        ]:
            if not self.runnable(identity, job):
                continue
            due = float(job.get("next_run", 0))
            if due > now:
                due_at = due if due_at is None else min(due_at, due)
                continue
            ready.append((identity, job))
        eligible = {i for i, _ in ready}
        self.queued_at = {i: value for i, value in self.queued_at.items() if i in eligible}
        for identity, job in ready:
            self.track_queue(identity, job)
        observed = time.monotonic()
        return sorted(ready, key=lambda pair: self.priority(*pair, now=observed)), due_at

    def track_queue(self, identity: DocumentId, job: dict[str, Any]) -> None:
        if self.runnable(identity, job) and float(job.get("next_run", 0)) <= time.time():
            if self.queued_at.get(identity, (None,))[0] != job["revision"]:
                self.queued_at[identity] = (job["revision"], time.monotonic())
        else:
            self.queued_at.pop(identity, None)

    def priority(
        self, identity: DocumentId, job: dict[str, Any], *, now: float
    ) -> tuple[int, float, float, str]:
        queued = self.queued_at.get(identity)
        start = queued[1] if queued else now
        age = max(0, now - start) / 60.0 if queued else 0.0
        return (
            0 if job.get("cleanup") or job["kind"] in ("drop", "delete", "rebuild") else 1,
            0.0 if job.get("force") else max(1.0, self.base_priority(identity, job) - age),
            start,
            str(identity),
        )

    def fail_job(self, identity: DocumentId, job: dict[str, Any], error: Exception) -> None:
        with self.condition:
            if not self.current(identity, job):
                return
            # Handlers may have changed their local stage before a transaction rolled back.
            # Resume the durable stage, or adopt a transaction that committed before raising.
            previous = self.work_target(identity, job)
            assert previous is not None
            try:
                durable = self.load_work(identity, job)
            except Exception:
                durable = None
            if durable is not None and durable != previous:
                self.remember(identity, durable)
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
            self.persist(identity, job)

    def save_prepared(
        self, identity: DocumentId, job: dict[str, Any], artifact: str, record: dict[str, Any]
    ) -> None:
        with self.condition:
            if not self.current(identity, job):
                return
            with self.catalog.transaction():
                self.catalog.execute(
                    "INSERT INTO prepared VALUES(?,?) ON CONFLICT(revision) "
                    "DO UPDATE SET path=excluded.path",
                    (job["revision"], artifact),
                )
                self.catalog.set_references(
                    "prepared", "", job["revision"], {artifact, *self.catalog.references(record)}
                )

    def claim(
        self,
        bound: Mapping[str, NamespaceBinding],
        unavailable: Container[str],
        admission: Callable[[DocumentId, dict[str, Any]], ResourceLease | None] | None = None,
    ) -> ExecutionPermit | None:
        with self.condition:
            self.bound, self.unavailable = bound, unavailable
            while not self.stopping:
                now = time.time()
                candidates, due_at = self.candidates()
                for identity, current in candidates:
                    job = copy.deepcopy(current)
                    job.setdefault("identity", asdict(identity))
                    try:
                        lease = admission(identity, job) if admission is not None else None
                    except Exception as error:
                        self.persist(
                            identity,
                            dict(
                                job,
                                state="failed",
                                error=str(error),
                                error_code=type(error).__name__,
                                retryable=False,
                            ),
                        )
                        with self.catalog.transaction():
                            self.catalog.put_active(identity.namespace, identity.doc_id, None)
                        self.active.pop(identity, None)
                        continue
                    if admission is not None and lease is None:
                        continue
                    if job.get("cleanup"):
                        job["cleanup_restore_state"] = (
                            "cancelled"
                            if self.catalog.cancelled(identity.namespace, identity.doc_id)
                            else "pending"
                        )
                    job.update(
                        state="cancelled"
                        if job.get("cleanup_restore_state") == "cancelled"
                        else "running",
                        attempts=int(job.get("attempts", 0)) + 1,
                        attempt_token=uuid.uuid4().hex,
                        index_epoch=job.get("build_generation")
                        or self.namespaces.get(identity.namespace, {}).get("index_epoch"),
                        collection_generation=job.get("build_generation")
                        or self.namespaces.get(identity.namespace, {}).get("active_generation"),
                        indexing=job.get("indexing")
                        if job.get("build_generation")
                        else self.namespaces.get(identity.namespace, {}).get("indexing"),
                    )
                    job.setdefault("active_run_id", uuid.uuid4().hex)
                    job.setdefault("identity", asdict(identity))
                    try:
                        self.persist(identity, job)
                    except Exception:
                        if lease is not None:
                            lease.release()
                        self.condition.wait(0.25)
                        break
                    token = str(job["attempt_token"])
                    cancellation = Cancellation(self.stage_timeout)
                    self.queued_at.pop(identity, None)
                    self.executing.add((identity, token))
                    self.execution_records[(identity, token)] = copy.deepcopy(job)
                    self.cancellations[(identity, token)] = cancellation
                    self.last_activity = time.monotonic()
                    subject = (
                        NamespaceWork(
                            identity.namespace,
                            str(job["revision"]),
                            job["kind"],
                        )
                        if job["kind"] in ("drop", "rebuild")
                        else FileWork(identity, str(job["revision"]))
                    )
                    return ExecutionPermit(
                        subject,
                        job.get("incarnation"),
                        token,
                        cancellation,
                        job,
                        self.generation_bindings.get((identity.namespace, job["build_generation"]))
                        if job.get("build_generation")
                        else bound.get(identity.namespace),
                        lease,
                    )
                else:
                    self.condition.wait(
                        0.05 if due_at is None else min(0.05, max(0.01, due_at - now))
                    )
            return None

    def finish_execution(
        self, permit: ExecutionPermit, result: StepResult | None, error: BaseException | None
    ) -> bool:
        """Called only after the adapter stack has exited; retain its lease until durable."""
        with self.condition:
            if error is None:
                try:
                    permit.cancellation.check()
                except BaseException as stopped:
                    error = stopped
            for attempt in range(3):
                try:
                    committed = False
                    if self.current(permit.identity, permit.payload):
                        if error is not None and not isinstance(
                            error, (_ProcessingStopped, _ProcessingYielded)
                        ):
                            self.fail_job(
                                permit.identity,
                                permit.payload,
                                error
                                if isinstance(error, Exception)
                                else ProcessingFailed(f"processor aborted: {error!r}"),
                            )
                        elif (
                            error is not None
                            or permit.cancellation._yield_requested
                            or permit.cancellation.reason in ("pause", "quiesce")
                            or self.stopping
                        ):
                            current = self.work_target(permit.identity, permit.payload)
                            assert current is not None
                            job = dict(current, state="pending", next_run=0)
                            self.persist(permit.identity, job)
                        elif result is not None:
                            committed = self.commit(permit, result)
                    self.retire(permit)
                    return committed
                except Exception as persistence_error:
                    # A lost acknowledgement may already have committed. Reconcile before
                    # retrying the transition, including a completion that removed plan data.
                    try:
                        durable = self.load_work(permit.identity, permit.payload)
                        if durable is not None and durable != self.work_target(
                            permit.identity, permit.payload
                        ):
                            if permit.payload["kind"] == "rebuild":
                                namespace = permit.identity.namespace
                                record = self.catalog.get_namespace(namespace)
                                if record is not None:
                                    self.namespaces[namespace] = record
                                    for ns, doc, target in self.catalog.list_targets():
                                        if ns == namespace:
                                            self.remember(DocumentId(ns, doc), target)
                            self.remember(permit.identity, durable)
                            if durable["state"] == "running":
                                self.persist(permit.identity, dict(durable, state="pending"))
                            self.retire(permit)
                            return result is not None and error is None
                    except Exception:
                        pass
                    if attempt == 2:
                        self.storage_error = StorageFailed(
                            f"execution completion could not persist: {persistence_error}"
                        )
                        self.stop()
                        if permit.lease is not None:
                            permit.lease.release()
                        return False
                    self.condition.wait(0.05 * (attempt + 1))
            return False

    def retire(self, permit: ExecutionPermit) -> None:
        with self.condition:
            target = self.work_target(permit.identity, permit.payload)
            if (
                target is None
                or target["revision"] != permit.subject.revision
                or target.get("active_run_id") != permit.payload.get("active_run_id")
                or target.get("build_generation") != permit.payload.get("build_generation")
                or target["state"] in ("succeeded", "cancelled", "failed", "blocked")
            ):
                with self.catalog.transaction():
                    self.catalog.put_active(permit.identity.namespace, permit.identity.doc_id, None)
                self.active.pop(permit.identity, None)
            execution = (permit.identity, permit.token)
            self.executing.discard(execution)
            self.execution_records.pop(execution, None)
            self.cancellations.pop(execution, None)
            if permit.lease is not None:
                permit.lease.release()
            self.last_activity = time.monotonic()
            self.condition.notify_all()

    def commit(self, permit: ExecutionPermit, result: StepResult) -> bool:
        identity, job = permit.identity, permit.payload
        with self.condition:
            current = self.work_target(identity, job)
            if (
                current is None
                or current["revision"] != permit.subject.revision
                or current.get("attempt_token") != permit.token
                or current.get("incarnation") != permit.incarnation
                or not self.current(identity, job)
            ):
                return False
            # An unchanged-content sync may refresh source stat while this permit
            # executes. Commit the stage onto the latest target, not its old copy.
            job = copy.deepcopy(current)
            if identity in self.progress:
                job["progress"] = self.progress[identity]
            if isinstance(result, Prepared):
                self.processed(identity, job, result.record)
            elif isinstance(result, NeedsPreparation):
                self.advance(identity, job, stage="process", refresh_text=True)
            elif isinstance(result, Chunked):
                assert permit.binding is not None
                with self.catalog.transaction():
                    prepared = (
                        self.catalog.get_build(identity.namespace, identity.doc_id, document=True)
                        if job.get("build_generation")
                        else self.catalog.get_document(identity.namespace, identity.doc_id)
                    )
                    if prepared is not None:
                        prepared.update(
                            chunk_plan=result.plan,
                            chunker=permit.binding.manifest["index"]["chunker"],
                        )
                        if job.get("build_generation"):
                            self.catalog.put_build(
                                identity.namespace, identity.doc_id, prepared, document=True
                            )
                        else:
                            self.catalog.put_document(identity.namespace, identity.doc_id, prepared)
                    self.advance(
                        identity,
                        job,
                        stage="embed" if result.plan else "publish",
                        plan=result.plan,
                        batches=(len(result.plan) + 127) // 128,
                        completed_batches=0,
                    )
            elif isinstance(result, Embedded):
                self.advance(
                    identity,
                    job,
                    completed_batches=result.completed_batches,
                    stage="publish" if result.final else "embed",
                )
            elif isinstance(result, Cleaned):
                job.pop("cleanup_restore_state", None)
                job.update(
                    cleanup=False,
                    state="cancelled"
                    if self.catalog.cancelled(identity.namespace, identity.doc_id)
                    else "pending",
                )
                self.persist(identity, job)
            elif isinstance(result, Published):
                job.update(
                    state="succeeded",
                    indexed_revision=job["revision"] if result.indexed else None,
                    published_artifacts=job.get("artifacts", {}),
                    error=None,
                    failures=0,
                )
                for field in ("plan", "snapshot", "chunks", "vectors"):
                    job.pop(field, None)
                self.persist(identity, job)
            else:
                self.finish_rebuild(identity, job)
            return True

    def finish_rebuild(self, identity: DocumentId, job: dict[str, Any]) -> None:
        record = dict(
            self.namespaces[identity.namespace], manifest=job["manifest"], legacy_collection=False
        )
        record.pop("pending_manifest", None)
        updates: list[tuple[DocumentId, dict[str, Any]]] = []
        with self.catalog.transaction():
            self.catalog.put_namespace(identity.namespace, record)
            for target_id, previous in self.targets.items():
                if target_id.namespace != identity.namespace or target_id == identity:
                    continue
                if previous["kind"] != "upsert":
                    target = dict(previous, state="succeeded", indexed_revision=None)
                elif previous["stage"] == "process":
                    continue
                else:
                    target = dict(
                        previous,
                        stage="chunk",
                        state="cancelled" if previous["state"] == "cancelled" else "pending",
                        vectors=[],
                        plan=[],
                        batches=0,
                        completed_batches=0,
                        index_epoch=record["index_epoch"],
                        indexed_revision=None,
                        error=None,
                        failures=0,
                        next_run=0,
                    )
                self.catalog.put_target(target_id.namespace, target_id.doc_id, target)
                updates.append((target_id, target))
            job.update(state="succeeded")
            self.catalog.put_target(identity.namespace, identity.doc_id, job)
        self.namespaces[identity.namespace] = record
        for target_id, target in updates:
            self.remember(target_id, target)
        self.remember(identity, job)

    @staticmethod
    def delete_job(kind: str = "delete") -> dict[str, Any]:
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

    def remove(self, identity: DocumentId) -> MutationReport:
        with self.condition:
            old = self.targets.get(identity)
            if old is None or old["kind"] != "upsert":
                return MutationReport(
                    identity,
                    "not_found",
                    self.is_ready(),
                    old["revision"] if old else None,
                )
            job = self.delete_job()
            job["incarnation"] = old.get("incarnation")
            job["indexed_revision"] = None
            job["published_artifacts"] = old.get("published_artifacts", {})
            with self.state_transaction():
                self.catalog.delete_document(identity.namespace, identity.doc_id)
                self.catalog.put_target(identity.namespace, identity.doc_id, job)
            self.visible.pop(identity, None)
            self.remember(identity, job)
            return MutationReport(identity, "removed", False, job["revision"])

    def cancel(self, document_id: DocumentId) -> None:
        with self.condition:
            previous = self.build_targets.get(document_id) or self.targets.get(document_id)
            if previous is None:
                raise InvalidQuery("document has no task")
            if previous["state"] == "succeeded":
                return
            if previous["kind"] in ("delete", "drop"):
                return
            job = copy.deepcopy(previous)
            job["state"] = "cancelled"
            job["attempt_token"] = uuid.uuid4().hex
            with self.state_transaction():
                self.catalog.set_cancelled(document_id.namespace, document_id.doc_id, True)
                self.store_work(document_id, job)
            self.remember(document_id, job)
            if job.get("build_generation"):
                current = self.targets.get(document_id)
                if current and current["state"] != "succeeded":
                    self.persist(
                        document_id,
                        dict(current, state="cancelled", attempt_token=uuid.uuid4().hex),
                    )

    def retry(self, document_id: DocumentId, stage: TaskStage | None = None) -> None:
        with self.condition:
            record = self.namespaces.get(document_id.namespace)
            if record and record.get("retirement_failures"):
                updated = dict(record, retirement_failures=0, retirement_retry=0)
                updated.pop("retirement_error", None)
                with self.state_transaction():
                    self.catalog.put_namespace(document_id.namespace, updated)
                self.namespaces[document_id.namespace] = updated
            with self.catalog.transaction():
                for key, debt in self.catalog.cleanup_rows(document_id.namespace):
                    if debt["doc_id"] == document_id.doc_id:
                        debt.update(failures=0, next_run=0, error=None)
                        self.catalog.execute(
                            "UPDATE index_cleanup SET value=? WHERE key=?",
                            (compact_json(debt), key),
                        )
            self.condition.notify_all()
            previous = self.build_targets.get(document_id) or self.targets.get(document_id)
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
            with self.state_transaction():
                self.catalog.set_cancelled(document_id.namespace, document_id.doc_id, False)
                self.store_work(document_id, job)
            self.remember(document_id, job)

            if job.get("build_generation"):
                serving = self.targets.get(document_id)
                if serving and serving["state"] == "cancelled":
                    self.persist(
                        document_id, dict(serving, state="pending", next_run=0, failures=0)
                    )

    def drop_namespace(self, namespace: str) -> DropReport:
        with self.condition:
            validate_namespace(namespace)
            if namespace not in self.namespaces:
                return DropReport(namespace, False, self.is_ready())
            # One durable namespace cleanup survives an immediate same-name recreation.
            identity = DocumentId(namespace, "")
            job = self.delete_job("drop")
            previous_drop = self.targets.get(identity, {})
            record = self.namespaces[namespace]
            job["collections"] = [
                *previous_drop.get("collections", []),
                *[
                    dict(incarnation=record["incarnation"], generation=g)
                    for g in {
                        record.get("active_generation"),
                        record.get("building", {}).get("generation"),
                        *record.get("retiring_generations", []),
                    }
                ],
            ]
            job["legacy_cleanup"] = bool(
                previous_drop.get("legacy_cleanup") or "manifest" not in self.namespaces[namespace]
            )
            job["published_artifacts"] = {
                str(index): path
                for index, path in enumerate(
                    p
                    for i, j in self.targets.items()
                    if i.namespace == namespace
                    for p in j.get("published_artifacts", {}).values()
                )
            }
            job["incarnations"] = list(
                dict.fromkeys(
                    [
                        *previous_drop.get("incarnations", []),
                        *previous_drop.get("retired_incarnations", []),
                        self.namespaces[namespace]["incarnation"],
                    ]
                )
            )
            for (identity_running, _), cancellation in self.cancellations.items():
                if identity_running.namespace == namespace:
                    cancellation._cancel("drop")
            with self.state_transaction():
                self.catalog.delete_namespace(namespace)
                self.catalog.delete_targets(namespace)
                for candidate in list(self.build_targets):
                    if candidate.namespace == namespace:
                        self.catalog.put_build(namespace, candidate.doc_id, None)
                        self.catalog.put_build(namespace, candidate.doc_id, None, document=True)
                self.catalog.put_target(namespace, "", job)
            self.namespaces.pop(namespace)
            self.visible = {i: v for i, v in self.visible.items() if i.namespace != namespace}
            self.targets = {i: j for i, j in self.targets.items() if i.namespace != namespace}
            self.build_targets = {
                i: j for i, j in self.build_targets.items() if i.namespace != namespace
            }
            self.refresh_pending()
            self.remember(identity, job)
            return DropReport(namespace, True, False)

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
        with self.condition:
            self.require_modern_namespace(namespace)
            previous = self.namespaces[namespace]
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
            with self.state_transaction():
                self.catalog.put_namespace(namespace, record)
                for identity, target in self.targets.items():
                    if (
                        identity.namespace != namespace
                        or target["kind"] != "upsert"
                        or not excluded(ordered, identity.doc_id)
                    ):
                        continue
                    deletion = dict(
                        self.delete_job(),
                        incarnation=target["incarnation"],
                        identity=asdict(identity),
                    )
                    self.catalog.delete_document(namespace, identity.doc_id)
                    self.catalog.put_target(namespace, identity.doc_id, deletion)
                    updates.append((identity, deletion))
            self.namespaces[namespace] = record
            for identity, deletion in updates:
                self.remember(identity, deletion)
            self.condition.notify_all()
            return RuleSet(record["rules_revision"], ordered)

    def accept(
        self,
        identity: DocumentId,
        source: SourceInput,
        artifacts: ArtifactStore,
        *,
        idempotency_key: str | None = None,
        force: bool = False,
    ) -> MutationReport:
        with self.condition:
            if self.stopping:
                raise Closed("MFS is closing")
            ns = self.namespaces[identity.namespace]
            if source.namespace_incarnation is not None and (
                source.namespace_incarnation != ns["incarnation"]
            ):
                raise Superseded("namespace was replaced while copying this input")
            fingerprint = dict(
                content_hash=source.content_hash,
                media_type=source.media_type,
                processor=source.processor,
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
                prior = self.catalog.get_operation(idempotency_key)
                if prior is not None:
                    if prior[0] != request_hash:
                        raise IdempotencyConflict(
                            "idempotency key was used for a different request"
                        )
                    r = prior[1]
                    return MutationReport(identity, r["outcome"], r["index_ready"], r["revision"])
            previous = self.targets.get(identity)
            unchanged = (
                not force
                and previous is not None
                and previous["kind"] == "upsert"
                and all(previous.get(k) == v for k, v in fingerprint.items())
            )
            if unchanged:
                assert previous is not None
                report = MutationReport(
                    identity,
                    "unchanged",
                    self.is_ready(),
                    previous["revision"],
                )
                with self.state_transaction():
                    refreshed = None
                    if ns["kind"] == "external":
                        observed = dict(
                            previous["source"], size=source.size, mtime_ns=source.mtime_ns
                        )
                        if observed != previous["source"]:
                            refreshed = dict(previous, source=observed)
                            self.catalog.put_target(identity.namespace, identity.doc_id, refreshed)
                    if idempotency_key is not None:
                        self.catalog.put_operation(idempotency_key, request_hash, asdict(report))
                if refreshed is not None:
                    # This observation changes no task or publication eligibility.
                    self.targets[identity] = copy.deepcopy(refreshed)
                return report
            revision = source.original_revision or uuid.uuid4().hex
            external = ns["kind"] == "external"
            object_name = (
                str(source.path)
                if external
                else source.path.relative_to(artifacts.root).as_posix()
                if source.original_revision
                else artifacts.accept_original(source.path, ns["incarnation"], revision)
            )
            source_record = dict(
                size=source.size,
                mtime_ns=source.mtime_ns if external else None,
                object=None if external else object_name,
                path=object_name if external else None,
            )
            job = dict(
                revision=revision,
                input_version=revision,
                identity=asdict(identity),
                force=force,
                enqueued_at=time.time(),
                published_artifacts={},
                cleanup=False,
                borrowed_input=external,
                kind="upsert",
                stage="process",
                state="pending",
                attempts=0,
                failures=0,
                next_run=0,
                error=None,
                input=object_name,
                source=source_record,
                incarnation=ns["incarnation"],
                indexed_revision=None,
                vectors=[],
                **fingerprint,
            )
            if not force and self.catalog.cancelled(identity.namespace, identity.doc_id):
                job["state"] = "cancelled"
            existed = previous is not None and previous["kind"] == "upsert"
            report = MutationReport(identity, "updated" if existed else "added", False, revision)
            with self.state_transaction():
                if force:
                    self.catalog.set_cancelled(identity.namespace, identity.doc_id, False)
                self.catalog.delete_document(identity.namespace, identity.doc_id)
                self.catalog.put_target(identity.namespace, identity.doc_id, job)
                if idempotency_key is not None:
                    self.catalog.put_operation(idempotency_key, request_hash, asdict(report))
            self.visible.pop(identity, None)
            self.remember(identity, job)
            return report

    def configure(self, namespace: str, record: dict[str, Any]) -> None:
        with self.condition:
            record = dict(record, index_epoch=record.get("index_epoch", record["incarnation"]))
            with self.state_transaction():
                self.catalog.put_namespace(namespace, record)
            self.namespaces[namespace] = copy.deepcopy(record)
            for identity, job in self.targets.items():
                if identity.namespace == namespace:
                    self.track_queue(identity, job)
            self.condition.notify_all()

    def resume_blocked(self, namespace: str) -> None:
        with self.condition:
            for identity, previous in list(self.targets.items()):
                if identity.namespace == namespace and previous["state"] == "blocked":
                    self.persist(identity, dict(previous, state="pending", error=None))
            self.condition.notify_all()

    def report_progress(
        self, identity: DocumentId, job: dict[str, Any], value: dict[str, Any], *, persist: bool
    ) -> None:
        with self.condition:
            self.check_execution(identity, job)
            self.progress[identity] = value
            if persist:
                target = self.work_target(identity, job)
                assert target is not None
                self.persist(identity, dict(target, progress=value))

    def checkpoint(
        self, identity: DocumentId, job: dict[str, Any], state: JSONValue, files: dict[str, str]
    ) -> bool:
        with self.condition:
            self.check_execution(identity, job)
            target = self.work_target(identity, job)
            assert target is not None
            job = copy.deepcopy(target)
            job["checkpoint"] = dict(state=state, files=files)
            if identity in self.progress:
                job["progress"] = self.progress[identity]
            self.persist(identity, job)
            candidates, _ = self.candidates()
            now = time.monotonic()
            should_yield = (
                bool(candidates)
                and self.priority(*candidates[0], now=now)[:2]
                < self.priority(identity, job, now=now)[:2]
            )
            cancellation = self.cancellations[(identity, job["attempt_token"])]
            if should_yield:
                candidate_id, candidate = candidates[0]
                should_yield = (
                    self.priority(candidate_id, candidate, now=now)[0] == 0
                    or self.base_priority(candidate_id, candidate)
                    < self.base_priority(identity, job)
                    or time.monotonic() - cancellation._started_at >= 0.05
                )
            if should_yield:
                cancellation._yield_requested = True
            return should_yield

    def namespace_record(self, namespace: str) -> dict[str, Any]:
        with self.condition:
            return copy.deepcopy(self.namespaces[namespace])

    def target_record(self, identity: DocumentId) -> dict[str, Any]:
        with self.condition:
            return copy.deepcopy(self.targets[identity])

    def cancellation_for(self, identity: DocumentId, job: dict[str, Any]) -> Cancellation:
        with self.condition:
            self.check_execution(identity, job)
            return self.cancellations[(identity, job["attempt_token"])]

    def visible_snapshot(self, identity: DocumentId, snapshot: str) -> bool:
        with self.condition:
            return self.visible.get(identity) == snapshot

    def check_execution(self, identity: DocumentId, job: dict[str, Any]) -> None:
        with self.condition:
            if not self.current(identity, job) or self.stopping:
                raise _ProcessingStopped()
            self.cancellations[(identity, job["attempt_token"])].check()

    def retarget_root(self, namespace: str, root: str) -> tuple[DocumentId, ...]:
        with self.condition:
            record = dict(self.namespaces[namespace], root_actual=root, binding=uuid.uuid4().hex)
            updates: list[tuple[DocumentId, dict[str, Any]]] = []
            with self.state_transaction():
                self.catalog.put_namespace(namespace, record)
                for identity, previous in self.targets.items():
                    if identity.namespace == namespace and previous["kind"] == "upsert":
                        target = dict(self.delete_job(), incarnation=previous["incarnation"])
                        self.catalog.delete_document(namespace, identity.doc_id)
                        self.catalog.put_target(namespace, identity.doc_id, target)
                        updates.append((identity, target))
            self.namespaces[namespace] = record
            for identity, target in updates:
                self.remember(identity, target)
            return tuple(identity for identity, _ in updates)

    def migrate_namespace(
        self,
        namespace: str,
        record: dict[str, Any],
        updates: list[tuple[DocumentId, dict[str, Any]]],
        control: dict[str, Any],
    ) -> None:
        with self.condition:
            with self.state_transaction():
                self.catalog.put_namespace(namespace, record)
                for doc_id, _ in self.catalog.list_namespace_documents(namespace):
                    self.catalog.delete_document(namespace, doc_id)
                for identity, old in self.targets.items():
                    if identity.namespace == namespace and old.get("revision"):
                        self.catalog.clear_prepared(old["revision"])
                self.catalog.delete_targets(namespace)
                for identity, job in updates:
                    self.catalog.put_target(namespace, identity.doc_id, job)
                self.catalog.put_target(namespace, "", control)
            self.namespaces[namespace] = record
            self.targets = {i: j for i, j in self.targets.items() if i.namespace != namespace}
            self.visible = {i: v for i, v in self.visible.items() if i.namespace != namespace}
            self.refresh_pending()
            for identity, job in updates:
                self.remember(identity, job)
            self.remember(DocumentId(namespace, ""), control)


class ReadView:
    """Read-only lifecycle interface; readers cannot edit targets or publication state."""

    def __init__(self, lifecycle: Lifecycle, unavailable: Container[str]) -> None:
        self._lifecycle = lifecycle
        self._unavailable = unavailable
        self.condition = lifecycle.condition

    def namespace(self, namespace: str) -> dict[str, Any]:
        with self.condition:
            self.require_modern_namespace(namespace)
            return copy.deepcopy(self._lifecycle.namespaces[namespace])

    def require_modern_namespace(self, namespace: str) -> None:
        if self._lifecycle.storage_error is not None:
            raise self._lifecycle.storage_error
        if self._lifecycle.boot_paused:
            raise CapabilityUnavailable("host recovery has not released the startup gate")
        self._lifecycle.require_modern_namespace(namespace)

    @contextmanager
    def source_read(self, identity: DocumentId, revision: str | None) -> Generator[bool]:
        with ExitStack() as leases:
            with self.condition:
                if self._lifecycle.storage_error is not None:
                    raise self._lifecycle.storage_error
                current = self.current_text(identity, revision)
                if current:
                    leases.enter_context(self._lifecycle.source_read(identity))
                    record = self._lifecycle.catalog.get_document(
                        identity.namespace, identity.doc_id
                    )
                    if record is None or record["revision"] != revision:
                        record = self._lifecycle.catalog.get_build(
                            identity.namespace, identity.doc_id, document=True
                        )
                    if record is not None and self._lifecycle.pin_artifact is not None:
                        for path in self._lifecycle.catalog.references(record):
                            leases.callback(self._lifecycle.pin_artifact(path).release)
            yield current

    def current_text(self, identity: DocumentId, revision: str | None) -> bool:
        with self.condition:
            namespace = self._lifecycle.namespaces.get(identity.namespace)
            desired = self._lifecycle.targets.get(identity, {})
            candidate = self._lifecycle.build_targets.get(identity, {})
            return (
                namespace is not None
                and desired.get("kind") == "upsert"
                and (
                    desired.get("revision") == revision
                    or (
                        candidate.get("revision") == revision
                        and candidate.get("source_revision") == desired.get("revision")
                    )
                )
                and not excluded(
                    tuple(IgnoreRule(**r) for r in namespace.get("rules", [])), identity.doc_id
                )
            )

    def visible(
        self,
        identity: DocumentId,
        snapshot: str,
        captured: dict[str, Any] | None = None,
    ) -> bool:
        with self.condition:
            namespace = self._lifecycle.namespaces.get(identity.namespace)
            target = self._lifecycle.targets.get(identity, {})
            publication = (captured or {}).get("_publications", {}).get(identity)
            return (
                namespace is not None
                and namespace.get("indexing") != "off"
                and (
                    self._lifecycle.visible.get(identity) == snapshot
                    if captured is None
                    else (
                        namespace.get("incarnation") == captured["incarnation"]
                        and target.get("kind") == "upsert"
                        and publication
                        == (snapshot, target.get("input_version", target.get("revision")))
                    )
                )
                and "pending_manifest" not in namespace
                and not excluded(
                    tuple(IgnoreRule(**r) for r in namespace.get("rules", [])), identity.doc_id
                )
            )

    def has_visible(self, namespace: str) -> bool:
        with self.condition:
            return any(i.namespace == namespace for i in self._lifecycle.visible)

    def wait_text_ready(self, namespace: str, timeout: float | None) -> None:
        def complete() -> bool:
            lifecycle = self._lifecycle
            self.require_modern_namespace(namespace)
            if lifecycle.stopping:
                raise Closed("MFS instance is closing")
            pending = False
            building = lifecycle.namespaces[namespace].get("building")
            if building and building.get("error") and building.get("failures", 0) >= 5:
                raise OperationFailed(building["error"], state="failed")
            for identity, desired in lifecycle.targets.items():
                if identity.namespace != namespace or desired["kind"] != "upsert":
                    continue
                candidate = lifecycle.build_targets.get(identity)
                if building and (
                    candidate is None or candidate.get("source_revision") != desired["revision"]
                ):
                    pending = True
                    continue
                job = candidate or desired
                if job["stage"] != "process":
                    continue
                if job["state"] in ("failed", "blocked", "cancelled"):
                    raise OperationFailed(
                        job.get("error") or f"text preparation is {job['state']}",
                        revision=job["revision"],
                        state=job["state"],
                    )
                if (
                    job.get("build_generation")
                    and (namespace, job["build_generation"]) not in lifecycle.generation_bindings
                ):
                    raise OperationFailed("candidate processor needs binding", state="blocked")
                pending = True
            return not pending

        with self.condition:
            if not self.condition.wait_for(complete, timeout):
                raise WaitTimeout("text preparation has not completed")

    def wait_ready(self, timeout: float | None, namespaces: set[str] | None = None) -> None:
        from .errors import IndexUnavailable

        with self.condition:
            lifecycle = self._lifecycle
            selected = set(lifecycle.namespaces) if namespaces is None else namespaces
            for namespace in selected:
                self.require_modern_namespace(namespace)
            if any(
                n in self._unavailable
                and "pending_manifest" not in lifecycle.namespaces[n]
                and "building" not in lifecycle.namespaces[n]
                for n in selected
            ):
                raise IndexUnavailable("selected namespace collection requires explicit reindex")
            completed = self.condition.wait_for(
                lambda: not lifecycle.unfinished(namespaces, readiness=True), timeout
            )
            if not completed:
                raise WaitTimeout("selected namespaces have not completed indexing")
