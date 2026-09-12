# pyright: reportPrivateUsage=false
from __future__ import annotations

import copy
import threading
import time
import uuid
from collections.abc import Container, Mapping, Sequence
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

import blake3

from ._catalog import Catalog
from ._json import JSONValue, canonical_json
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
    Prepared,
    Published,
    SourceInput,
    StepResult,
)
from .errors import (
    CapabilityUnavailable,
    Closed,
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
    WaitTimeout,
)
from .processing import Cancellation, _ProcessingStopped, _ProcessingYielded
from .types import DocumentId, DropReport, IgnoreRule, MutationReport, RuleSet, TaskStage, UnderPath

if TYPE_CHECKING:
    from ._artifacts import ArtifactStore


class Lifecycle:
    """One owner for durable targets, live execution and publication eligibility.

    Callers hold condition for multi-record acceptance transactions. Slow work never
    holds it; its commit must validate the current revision and execution token.
    """

    def __init__(self, catalog: Catalog, condition: threading.Condition) -> None:
        self.catalog, self.condition = catalog, condition
        self.stopping = False
        self.active_scopes: tuple[UnderPath, ...] = ()
        self.readers = 0
        self.last_activity = time.monotonic()
        self.targets: dict[DocumentId, dict[str, Any]] = {}
        self.pending: dict[DocumentId, str] = {}
        self.visible: dict[DocumentId, str] = {}
        self.progress: dict[DocumentId, dict[str, Any]] = {}
        self.executing: set[tuple[DocumentId, str]] = set()
        self.cancellations: dict[tuple[DocumentId, str], Cancellation] = {}
        self.queries: dict[str, int] = {}
        self.queued_at: dict[DocumentId, tuple[str, float]] = {}
        self.bound: Mapping[str, NamespaceBinding] = {}
        self.unavailable: Container[str] = ()
        self.storage_error: StorageFailed | None = None
        self.namespaces = dict(catalog.list_namespaces())
        with catalog.transaction():
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

    def stop(self) -> None:
        with self.condition:
            self.stopping = True
            for cancellation in self.cancellations.values():
                cancellation._cancel("close")
            self.condition.notify_all()

    def wait(
        self,
        namespace: str,
        identity: DocumentId | None,
        path: str,
        timeout: float | None,
    ) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            while True:
                if self.storage_error is not None:
                    raise self.storage_error
                if self.stopping:
                    raise Closed("MFS instance is closing")
                pending = False
                for current, job in self.targets.items():
                    if current.namespace != namespace:
                        continue
                    # Namespace rebuild/drop work affects every file within the scope.
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
                if not pending:
                    return
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise WaitTimeout("current file/scope work has not completed")
                self.condition.wait(remaining)

    def remember(self, identity: DocumentId, job: dict[str, Any]) -> None:
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
        self.condition.notify_all()

    def persist(self, identity: DocumentId, job: dict[str, Any]) -> None:
        try:
            with self.catalog.transaction():
                self.catalog.put_target(identity.namespace, identity.doc_id, job)
        except Exception:
            # A committed checkpoint/claim can lose its acknowledgement. Adopt the
            # exact durable value so a running target never becomes an orphan.
            if self.catalog.get_target(identity.namespace, identity.doc_id) != job:
                raise
        self.remember(identity, job)

    def current(self, identity: DocumentId, job: dict[str, Any]) -> bool:
        current = self.targets.get(identity)
        return (
            current is not None
            and (identity, job.get("attempt_token", "")) in self.executing
            and current["revision"] == job["revision"]
            and (current["state"] != "cancelled" or bool(job.get("cleanup")))
            and current.get("attempt_token") == job.get("attempt_token")
            and (
                job["kind"] != "upsert"
                or job["stage"] == "process"
                or bool(job.get("cleanup"))
                or job.get("index_epoch")
                == self.namespaces.get(identity.namespace, {}).get("index_epoch")
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
            job = copy.deepcopy(self.targets[identity])
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
            with self.catalog.transaction():
                self.catalog.put_document(identity.namespace, identity.doc_id, record)
                self.catalog.put_target(identity.namespace, identity.doc_id, target)
                self.catalog.clear_prepared(job["revision"])
            self.remember(identity, target)

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
        if any(i == identity for i, _ in self.executing):
            return False
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
                incarnation is not None and self.queries.get(incarnation, 0)
                for incarnation in incarnations
            ):
                return False
        if job["kind"] == "upsert" and not job.get("cleanup"):
            if identity.namespace not in self.bound:
                return False
            namespace = self.namespaces[identity.namespace]
            if job["stage"] != "process" and (
                "pending_manifest" in namespace
                or (namespace["paused"] and namespace["indexing"] != "off")
                or identity.namespace in self.unavailable
            ):
                return False
        return True

    def candidates(self) -> tuple[list[tuple[DocumentId, dict[str, Any]]], float | None]:
        now, due_at = time.time(), None
        ready: list[tuple[DocumentId, dict[str, Any]]] = []
        for identity in self.pending:
            job = self.targets[identity]
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
            self.base_priority(identity, job) - age,
            start,
            str(identity),
        )

    def fail_job(self, identity: DocumentId, job: dict[str, Any], error: Exception) -> None:
        with self.condition:
            if not self.current(identity, job):
                return
            # Handlers may have changed their local stage before a transaction rolled back.
            # Resume the durable stage, or adopt a transaction that committed before raising.
            previous = self.targets[identity]
            try:
                durable = self.catalog.get_target(identity.namespace, identity.doc_id)
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
                self.catalog.connection.execute(
                    "INSERT INTO prepared VALUES(?,?) ON CONFLICT(revision) "
                    "DO UPDATE SET path=excluded.path",
                    (job["revision"], artifact),
                )
                self.catalog.set_references(
                    "prepared", "", job["revision"], {artifact, *self.catalog.references(record)}
                )

    def claim(
        self, bound: Mapping[str, NamespaceBinding], unavailable: Container[str]
    ) -> ExecutionPermit | None:
        with self.condition:
            self.bound, self.unavailable = bound, unavailable
            while not self.stopping:
                now = time.time()
                candidates, due_at = self.candidates()
                for identity, current in candidates:
                    job = copy.deepcopy(current)
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
                        index_epoch=self.namespaces.get(identity.namespace, {}).get("index_epoch"),
                        indexing=self.namespaces.get(identity.namespace, {}).get("indexing"),
                    )
                    job.setdefault("identity", asdict(identity))
                    try:
                        self.persist(identity, job)
                    except Exception:
                        self.condition.wait(0.25)
                        break
                    token = str(job["attempt_token"])
                    cancellation = Cancellation()
                    self.queued_at.pop(identity, None)
                    self.executing.add((identity, token))
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
                        bound.get(identity.namespace),
                    )
                else:
                    self.condition.wait(None if due_at is None else max(0.01, due_at - now))
            return None

    def finish_execution(
        self, permit: ExecutionPermit, result: StepResult | None, error: BaseException | None
    ) -> bool:
        """Called only after the adapter stack has exited; retain its lease until durable."""
        with self.condition:
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
                            or self.stopping
                        ):
                            job = dict(self.targets[permit.identity], state="pending", next_run=0)
                            self.persist(permit.identity, job)
                        elif result is not None:
                            committed = self.commit(permit, result)
                    self.retire(permit)
                    return committed
                except Exception as persistence_error:
                    # A lost acknowledgement may already have committed. Reconcile before
                    # retrying the transition, including a completion that removed plan data.
                    try:
                        durable = self.catalog.get_target(
                            permit.identity.namespace, permit.identity.doc_id
                        )
                        if durable is not None and durable != self.targets.get(permit.identity):
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
                        return False
                    self.condition.wait(0.05 * (attempt + 1))
            return False

    def retire(self, permit: ExecutionPermit) -> None:
        with self.condition:
            execution = (permit.identity, permit.token)
            self.executing.discard(execution)
            self.cancellations.pop(execution, None)
            self.last_activity = time.monotonic()
            self.condition.notify_all()

    def commit(self, permit: ExecutionPermit, result: StepResult) -> bool:
        identity, job = permit.identity, permit.payload
        with self.condition:
            current = self.targets.get(identity)
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
            elif isinstance(result, Chunked):
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
                job.update(cleanup=False, state=job.pop("cleanup_restore_state", "pending"))
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
                    not self.pending,
                    old["revision"] if old else None,
                )
            job = self.delete_job()
            job["incarnation"] = old.get("incarnation")
            job["indexed_revision"] = None
            job["published_artifacts"] = old.get("published_artifacts", {})
            with self.catalog.transaction():
                self.catalog.delete_document(identity.namespace, identity.doc_id)
                self.catalog.put_target(identity.namespace, identity.doc_id, job)
            self.visible.pop(identity, None)
            self.remember(identity, job)
            return MutationReport(identity, "removed", False, job["revision"])

    def cancel(self, document_id: DocumentId) -> None:
        with self.condition:
            previous = self.targets.get(document_id)
            if previous is None:
                raise InvalidQuery("document has no task")
            if previous["state"] == "succeeded":
                return
            if previous["kind"] in ("delete", "drop"):
                return
            job = copy.deepcopy(previous)
            job["state"] = "cancelled"
            job["attempt_token"] = uuid.uuid4().hex
            with self.catalog.transaction():
                self.catalog.set_cancelled(document_id.namespace, document_id.doc_id, True)
                self.catalog.put_target(document_id.namespace, document_id.doc_id, job)
            self.remember(document_id, job)

    def retry(self, document_id: DocumentId, stage: TaskStage | None = None) -> None:
        with self.condition:
            previous = self.targets.get(document_id)
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
            with self.catalog.transaction():
                self.catalog.set_cancelled(document_id.namespace, document_id.doc_id, False)
                self.catalog.put_target(document_id.namespace, document_id.doc_id, job)
            self.remember(document_id, job)

    def drop_namespace(self, namespace: str) -> DropReport:
        with self.condition:
            validate_namespace(namespace)
            if namespace not in self.namespaces:
                return DropReport(namespace, False, not self.pending)
            # One durable namespace cleanup survives an immediate same-name recreation.
            identity = DocumentId(namespace, "")
            job = self.delete_job("drop")
            previous_drop = self.targets.get(identity, {})
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
            with self.catalog.transaction():
                self.catalog.delete_namespace(namespace)
                self.catalog.delete_targets(namespace)
                self.catalog.put_target(namespace, "", job)
            self.namespaces.pop(namespace)
            self.visible = {i: v for i, v in self.visible.items() if i.namespace != namespace}
            self.targets = {i: j for i, j in self.targets.items() if i.namespace != namespace}
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
            with self.catalog.transaction():
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
                    not self.pending,
                    previous["revision"],
                )
                with self.catalog.transaction():
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
            revision = uuid.uuid4().hex
            external = ns["kind"] == "external"
            object_name = (
                str(source.path)
                if external
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
            try:
                with self.catalog.transaction():
                    if force:
                        self.catalog.set_cancelled(identity.namespace, identity.doc_id, False)
                    self.catalog.delete_document(identity.namespace, identity.doc_id)
                    self.catalog.put_target(identity.namespace, identity.doc_id, job)
                    if idempotency_key is not None:
                        self.catalog.put_operation(idempotency_key, request_hash, asdict(report))
            except Exception:
                # A lost ACK must not leave durable accepted work out of the live pending set.
                durable = self.catalog.get_target(identity.namespace, identity.doc_id)
                if durable is not None:
                    self.remember(identity, durable)
                raise
            self.visible.pop(identity, None)
            self.remember(identity, job)
            return report

    def request_rebuild(
        self, namespace: str, manifest: dict[str, Any], changes: dict[str, Any] | None = None
    ) -> None:
        with self.condition:
            record = dict(self.namespaces[namespace], **(changes or {}))
            record["index_epoch"] = uuid.uuid4().hex
            record["pending_manifest"] = manifest
            previous = self.targets.get(DocumentId(namespace, ""), {})
            job = dict(
                self.delete_job("rebuild"),
                identity=asdict(DocumentId(namespace, "")),
                incarnation=record["incarnation"],
                manifest=manifest,
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
            with self.catalog.transaction():
                self.catalog.put_namespace(namespace, record)
                self.catalog.put_target(namespace, "", job)
                self.catalog.clear_vector_cache(record["incarnation"])
            self.namespaces[namespace] = record
            self.visible = {i: v for i, v in self.visible.items() if i.namespace != namespace}
            self.remember(DocumentId(namespace, ""), job)

    def configure(self, namespace: str, record: dict[str, Any]) -> None:
        with self.condition:
            record = dict(record, index_epoch=record.get("index_epoch", record["incarnation"]))
            with self.catalog.transaction():
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
                self.persist(identity, dict(self.targets[identity], progress=value))

    def checkpoint(
        self, identity: DocumentId, job: dict[str, Any], state: JSONValue, files: dict[str, str]
    ) -> bool:
        with self.condition:
            self.check_execution(identity, job)
            job = copy.deepcopy(self.targets[identity])
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

    def retarget_root(self, namespace: str, root: str) -> tuple[DocumentId, ...]:
        with self.condition:
            record = dict(self.namespaces[namespace], root_actual=root, binding=uuid.uuid4().hex)
            updates: list[tuple[DocumentId, dict[str, Any]]] = []
            with self.catalog.transaction():
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
            with self.catalog.transaction():
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

    def reprocess_namespace(
        self, namespace: str, manifest: dict[str, Any], routes: dict[str, dict[str, Any]]
    ) -> tuple[MutationReport, ...]:
        previous = self.namespaces[namespace]
        with self.condition:
            record = dict(previous, manifest=manifest, binding=uuid.uuid4().hex)
            updates: list[tuple[DocumentId, dict[str, Any]]] = []
            reports: list[MutationReport] = []
            for identity, old in self.targets.items():
                if identity.namespace != namespace or old["kind"] != "upsert":
                    continue
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
                    processor=routes.get(old["media_type"]),
                    binding=record["binding"],
                    indexed_revision=None,
                    cleanup=True,
                    enqueued_at=time.time(),
                    force=True,
                )
                updates.append((identity, job))
                reports.append(MutationReport(identity, "updated", False, job["revision"]))
            with self.catalog.transaction():
                self.catalog.put_namespace(namespace, record)
                for identity, job in updates:
                    self.catalog.delete_document(namespace, identity.doc_id)
                    self.catalog.clear_prepared(self.targets[identity]["revision"])
                    self.catalog.set_cancelled(namespace, identity.doc_id, False)
                    self.catalog.put_target(namespace, identity.doc_id, job)
            self.namespaces[namespace] = record
            for identity, job in updates:
                self.remember(identity, job)
        return tuple(reports)


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
        self._lifecycle.require_modern_namespace(namespace)

    def current_text(self, identity: DocumentId, revision: str | None) -> bool:
        with self.condition:
            namespace = self._lifecycle.namespaces.get(identity.namespace)
            return (
                namespace is not None
                and self._lifecycle.targets.get(identity, {}).get("revision") == revision
                and not excluded(
                    tuple(IgnoreRule(**r) for r in namespace.get("rules", [])), identity.doc_id
                )
            )

    def visible(self, identity: DocumentId, snapshot: str) -> bool:
        with self.condition:
            namespace = self._lifecycle.namespaces.get(identity.namespace)
            return (
                namespace is not None
                and self._lifecycle.visible.get(identity) == snapshot
                and "pending_manifest" not in namespace
                and not excluded(
                    tuple(IgnoreRule(**r) for r in namespace.get("rules", [])), identity.doc_id
                )
            )

    def wait_ready(self, timeout: float | None, namespaces: set[str] | None = None) -> None:
        from .errors import IndexUnavailable

        with self.condition:
            lifecycle = self._lifecycle
            selected = set(lifecycle.namespaces) if namespaces is None else namespaces
            for namespace in selected:
                self.require_modern_namespace(namespace)
            if any(
                n in self._unavailable and "pending_manifest" not in lifecycle.namespaces[n]
                for n in selected
            ):
                raise IndexUnavailable("selected namespace collection requires explicit reindex")
            completed = self.condition.wait_for(
                lambda: (
                    lifecycle.stopping
                    or not any(
                        namespaces is None or identity.namespace in selected
                        for identity in lifecycle.pending
                    )
                ),
                timeout,
            )
            if lifecycle.stopping:
                if lifecycle.storage_error is not None:
                    raise lifecycle.storage_error
                raise Closed("MFS instance is closing")
            if not completed:
                raise WaitTimeout("selected namespaces have not completed indexing")
