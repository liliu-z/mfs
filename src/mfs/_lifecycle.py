# pyright: reportPrivateUsage=false
from __future__ import annotations

import copy
import threading
import uuid
from dataclasses import asdict
from typing import Any

from ._catalog import Catalog
from ._rules import excluded
from .processing import Cancellation
from .types import DocumentId, IgnoreRule


class Lifecycle:
    """One owner for durable targets, live execution and publication eligibility.

    Callers hold condition for multi-record acceptance transactions. Slow work never
    holds it; its commit must validate the current revision and execution token.
    """

    def __init__(self, catalog: Catalog, condition: threading.Condition) -> None:
        self.catalog, self.condition = catalog, condition
        self.targets: dict[DocumentId, dict[str, Any]] = {}
        self.pending: dict[DocumentId, str] = {}
        self.visible: dict[DocumentId, str] = {}
        self.progress: dict[DocumentId, dict[str, Any]] = {}
        self.executing: set[tuple[DocumentId, str]] = set()
        self.cancellations: dict[tuple[DocumentId, str], Cancellation] = {}
        self.namespaces = dict(catalog.list_namespaces())
        with catalog.transaction():
            for name, record in self.namespaces.items():
                record.setdefault("incarnation", uuid.uuid4().hex)
                record.setdefault("binding", uuid.uuid4().hex)
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

    def refresh_pending(self) -> None:
        self.pending = {
            identity: str(job["revision"])
            for identity, job in self.targets.items()
            if job["state"] != "succeeded"
        }

    def remember(self, identity: DocumentId, job: dict[str, Any]) -> None:
        if job["kind"] != "upsert" or job.get("indexed_revision") != job["revision"]:
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
        self.condition.notify_all()

    def persist(self, identity: DocumentId, job: dict[str, Any]) -> None:
        with self.catalog.transaction():
            self.catalog.put_target(identity.namespace, identity.doc_id, job)
        self.remember(identity, job)

    def current(self, identity: DocumentId, job: dict[str, Any]) -> bool:
        current = self.targets.get(identity)
        return (
            current is not None
            and current["revision"] == job["revision"]
            and (current["state"] != "cancelled" or bool(job.get("cleanup")))
            and current.get("attempt_token") == job.get("attempt_token")
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
