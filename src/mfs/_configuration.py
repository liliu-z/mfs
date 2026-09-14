# pyright: reportPrivateUsage=false
from __future__ import annotations

import copy
import time
import uuid
from typing import Any

from ._lifecycle import Lifecycle
from ._namespace import NamespaceBinding
from ._runtime import NamespaceRuntime
from .errors import IndexUnavailable
from .types import ConfigurationReport, DocumentId


class Configuration:
    """Build one candidate configuration while the active generation keeps serving."""

    def __init__(self, lifecycle: Lifecycle, runtime: NamespaceRuntime) -> None:
        self.lifecycle, self.runtime = lifecycle, runtime
        self.catalog = lifecycle.catalog
        self.creating: set[str] = set()
        with lifecycle.condition, self.catalog.transaction():
            for namespace, record in lifecycle.namespaces.items():
                if record.get("building"):
                    record["building"]["initialized"] = False
                    self.catalog.put_namespace(namespace, record)

    def request(
        self,
        namespace: str,
        binding: NamespaceBinding,
        mode: str,
        *,
        force: bool = False,
        force_process: bool = False,
    ) -> ConfigurationReport:
        lifecycle = self.lifecycle
        with lifecycle.condition:
            previous = lifecycle.namespaces[namespace]
            if "pending_manifest" in previous:
                raise IndexUnavailable(
                    "finish or retry the legacy migration before changing configuration"
                )
            building = previous.get("building")
            if (
                not force
                and (building or previous)["manifest"] == binding.manifest
                and (building or previous)["indexing"] == mode
            ):
                generation = (building or previous).get("generation", previous["index_epoch"])
                if building and building.get("error"):
                    record = copy.deepcopy(previous)
                    for field in ("error", "failures", "next_run"):
                        record["building"].pop(field, None)
                    with lifecycle.state_transaction():
                        self.catalog.put_namespace(namespace, record)
                    lifecycle.namespaces[namespace] = record
                if building:
                    self.runtime.build_bindings[(namespace, generation)] = binding
                else:
                    self.runtime.bindings[namespace] = binding
                return ConfigurationReport(namespace, generation, False)
            generation = uuid.uuid4().hex
            record = copy.deepcopy(previous)
            for field in ("retirement_error", "retirement_failures", "retirement_retry"):
                record.pop(field, None)
            if building and (building["initialized"] or building["generation"] in self.creating):
                record.setdefault("retiring_generations", []).append(building["generation"])
            record["building"] = dict(
                generation=generation,
                manifest=binding.manifest,
                indexing=mode,
                binding=uuid.uuid4().hex,
                initialized=False,
                force_process=force_process,
            )
            if mode == "off":
                record["indexing"] = "off"
            self.runtime.build_bindings[(namespace, generation)] = binding
            with lifecycle.state_transaction():
                self.catalog.put_namespace(namespace, record)
                if force_process:
                    self.catalog.execute("DELETE FROM cancel_gates WHERE namespace=?", (namespace,))
                for identity in list(lifecycle.build_targets):
                    if identity.namespace == namespace:
                        self.catalog.put_build(namespace, identity.doc_id, None)
                        self.catalog.put_build(namespace, identity.doc_id, None, document=True)
            lifecycle.namespaces[namespace] = record
            if building:
                self.runtime.build_bindings.pop((namespace, building["generation"]), None)
            for identity in list(lifecycle.build_targets):
                if identity.namespace == namespace:
                    lifecycle.build_targets.pop(identity)
            self.runtime.build_bindings[(namespace, generation)] = binding
            for (identity, _), cancellation in lifecycle.cancellations.items():
                active = lifecycle.active.get(identity, {})
                if identity.namespace == namespace and (
                    active.get("build_generation")
                    or (
                        mode == "off"
                        and active.get("kind") == "upsert"
                        and active.get("stage") != "process"
                    )
                ):
                    cancellation._cancel("superseded")
            # The candidate declaration is the durable intent. Membership expansion
            # is recovered in bounded maintenance batches, including after a write
            # failure or process exit between accepting the declaration and its members.
            lifecycle.condition.notify_all()
            return ConfigurationReport(namespace, generation, True)

    def reconcile_members(self, namespace: str, generation: str) -> None:
        lifecycle = self.lifecycle
        with lifecycle.condition:
            building = lifecycle.namespaces.get(namespace, {}).get("building", {})
            if building.get("generation") != generation:
                return
            rows = self.catalog.query(
                "SELECT t.doc_id FROM targets t LEFT JOIN build_targets b "
                "ON b.namespace=t.namespace AND b.doc_id=t.doc_id "
                "LEFT JOIN documents d ON d.namespace=t.namespace AND d.doc_id=t.doc_id "
                "LEFT JOIN cancel_gates c ON c.namespace=t.namespace AND c.doc_id=t.doc_id "
                "WHERE t.namespace=? AND t.doc_id!='' "
                "AND ((json_extract(t.value,'$.kind')!='upsert' AND b.doc_id IS NOT NULL) OR "
                "(json_extract(t.value,'$.kind')='upsert' AND "
                "(b.doc_id IS NULL OR json_extract(b.value,'$.source_revision') IS NOT t.revision "
                "OR json_extract(b.value,'$.build_generation') IS NOT ? "
                "OR (c.doc_id IS NOT NULL "
                "AND json_extract(b.value,'$.state') NOT IN ('cancelled','succeeded')) "
                "OR (json_extract(b.value,'$.stage')='process' "
                "AND json_extract(b.value,'$.state')!='running' "
                "AND NOT coalesce(json_extract(b.value,'$.refresh_text'),0) AND NOT ? "
                "AND json_extract(b.value,'$.processor') IS json_extract(t.value,'$.processor') "
                "AND json_extract(d.value,'$.revision')=t.revision)))) "
                "ORDER BY t.doc_id LIMIT 32",
                (namespace, generation, bool(building.get("force_process"))),
            )
        for (doc_id,) in rows:
            with lifecycle.condition:
                if (
                    lifecycle.namespaces.get(namespace, {}).get("building", {}).get("generation")
                    != generation
                    or lifecycle.stopping
                ):
                    return
                self.synchronize(DocumentId(namespace, doc_id))

    def synchronize(self, identity: DocumentId) -> None:
        lifecycle = self.lifecycle
        record = lifecycle.namespaces.get(identity.namespace, {})
        building = record.get("building")
        if not building or not identity.doc_id:
            return
        desired = self.catalog.get_target(identity.namespace, identity.doc_id) or {}
        previous = lifecycle.build_targets.get(identity)
        if desired.get("kind") != "upsert":
            self.store_member(identity, None, None)
            return
        cancelled = self.catalog.cancelled(identity.namespace, identity.doc_id)
        if (
            previous
            and previous.get("source_revision") == desired["revision"]
            and previous.get("build_generation") == building["generation"]
        ):
            if cancelled and previous["state"] not in ("cancelled", "succeeded"):
                job = dict(previous, state="cancelled", attempt_token=uuid.uuid4().hex)
                lifecycle.persist(identity, job)
            elif (
                previous["stage"] == "process"
                and previous["state"] != "running"
                and not previous.get("refresh_text")
                and previous.get("processor") == desired.get("processor")
                and not building.get("force_process")
            ):
                prepared = self.catalog.get_document(identity.namespace, identity.doc_id)
                if prepared and prepared["revision"] == desired["revision"]:
                    job = dict(
                        previous,
                        stage="chunk",
                        snapshot_id=prepared["snapshot_id"],
                        text_ref=prepared.get("text_ref"),
                        artifacts=prepared.get("artifacts", {}),
                    )
                    self.store_member(identity, job, prepared)
            return
        processor = next(
            (
                {k: p[k] for k in ("id", "version", "options")}
                for p in building["manifest"]["processors"]
                if desired["media_type"] in p["media_types"]
            ),
            None,
        )
        same_processor = processor == desired.get("processor") and not building.get("force_process")
        job = {
            k: copy.deepcopy(desired[k])
            for k in (
                "identity",
                "input",
                "borrowed_input",
                "source",
                "incarnation",
                "content_hash",
                "input_version",
                "media_type",
            )
            if k in desired
        }
        job.update(
            revision=desired["revision"] if same_processor else uuid.uuid4().hex,
            input_version=desired.get("input_version", desired["revision"]),
            source_revision=desired["revision"],
            processor=processor,
            binding=building["binding"],
            build_generation=building["generation"],
            collection_generation=building["generation"],
            index_epoch=building["generation"],
            indexing=building["indexing"],
            kind="upsert",
            stage="process",
            state="cancelled" if cancelled else "pending",
            cleanup=False,
            indexed_revision=None,
            attempts=0,
            failures=0,
            next_run=0,
            error=None,
            enqueued_at=time.time(),
            active_run_id=uuid.uuid4().hex,
            force=bool(building.get("force_process")),
        )
        prepared = self.catalog.get_document(identity.namespace, identity.doc_id)
        if not same_processor or not prepared or prepared["revision"] != desired["revision"]:
            prepared = None
        if prepared:
            job.update(
                stage="chunk",
                snapshot_id=prepared["snapshot_id"],
                text_ref=prepared.get("text_ref"),
                artifacts=prepared.get("artifacts", {}),
            )
        self.store_member(identity, job, prepared)

    def store_member(
        self, identity: DocumentId, job: dict[str, Any] | None, prepared: dict[str, Any] | None
    ) -> None:
        try:
            with self.lifecycle.state_transaction():
                self.catalog.put_build(identity.namespace, identity.doc_id, job)
                self.catalog.put_build(identity.namespace, identity.doc_id, prepared, document=True)
        except Exception:
            # A lost commit acknowledgement must not leave a durable member absent
            # from the runnable in-memory view. Adopt only the exact atomic pair.
            if (
                self.catalog.get_build(identity.namespace, identity.doc_id) != job
                or self.catalog.get_build(identity.namespace, identity.doc_id, document=True)
                != prepared
            ):
                raise
        if job is None:
            self.lifecycle.build_targets.pop(identity, None)
        else:
            self.lifecycle.remember(identity, job)

    def maintain(self) -> None:
        """One maintenance owner; a failed namespace does not stall other builds."""
        lifecycle = self.lifecycle
        with lifecycle.condition:
            records = [
                (name, copy.deepcopy(record)) for name, record in lifecycle.namespaces.items()
            ]
        for namespace, record in records:
            try:
                self.retire(namespace)
            except Exception as error:
                if lifecycle.storage_error is not None:
                    raise lifecycle.storage_error from error
                with lifecycle.condition:
                    current = lifecycle.namespaces.get(namespace)
                    if current is not None:
                        failures = int(current.get("retirement_failures", 0)) + 1
                        updated = dict(
                            current,
                            retirement_error=str(error),
                            retirement_failures=failures,
                            retirement_retry=time.time() + min(30, 0.25 * 2 ** min(failures, 7)),
                        )
                        with lifecycle.state_transaction():
                            self.catalog.put_namespace(namespace, updated)
                        lifecycle.namespaces[namespace] = updated
                        lifecycle.condition.notify_all()
            try:
                building = record.get("building")
                if building and (
                    building.get("failures", 0) >= 5 or building.get("next_run", 0) > time.time()
                ):
                    continue
                if building:
                    self.reconcile_members(namespace, building["generation"])
                self.initialize(namespace, record)
                with lifecycle.condition:
                    self.promote(namespace)
                    current = lifecycle.namespaces.get(namespace, {})
                    current_build = current.get("building")
                    if (
                        current_build
                        and current_build["generation"]
                        == record.get("building", {}).get("generation")
                        and any(f in current_build for f in ("error", "failures", "next_run"))
                    ):
                        updated = copy.deepcopy(current)
                        for field in ("error", "failures", "next_run"):
                            updated["building"].pop(field, None)
                        with lifecycle.state_transaction():
                            self.catalog.put_namespace(namespace, updated)
                        lifecycle.namespaces[namespace] = updated
            except Exception as error:
                if lifecycle.storage_error is not None:
                    raise lifecycle.storage_error from error
                with lifecycle.condition:
                    current = lifecycle.namespaces.get(namespace, {})
                    building = current.get("building")
                    if not building or building.get("generation") != record.get("building", {}).get(
                        "generation"
                    ):
                        continue
                    updated = copy.deepcopy(current)
                    failures = int(building.get("failures", 0)) + 1
                    updated["building"].update(
                        error=str(error),
                        failures=failures,
                        next_run=time.time() + min(30, 0.25 * 2 ** min(failures, 7)),
                    )
                    with lifecycle.state_transaction():
                        self.catalog.put_namespace(namespace, updated)
                    lifecycle.namespaces[namespace] = updated
                    lifecycle.condition.notify_all()

    def initialize(self, namespace: str, record: dict[str, Any]) -> None:
        lifecycle = self.lifecycle
        building = record.get("building")
        if not building or building["initialized"]:
            return
        if building.get("failures", 0) >= 5 or building.get("next_run", 0) > time.time():
            return
        generation = building["generation"]
        with lifecycle.condition:
            current = lifecycle.namespaces.get(namespace, {})
            if current.get("building", {}).get("generation") != generation:
                return
            if lifecycle.held(DocumentId(namespace, ""), current):
                return
            if len(current.get("retiring_generations", [])) >= 2 or any(
                j.get("build_generation")
                and j["build_generation"] != generation
                and j.get("incarnation") == record["incarnation"]
                for j in lifecycle.execution_records.values()
            ):
                return
            self.creating.add(generation)
            incarnation = record["incarnation"]
            lifecycle.namespace_executions[incarnation] = (
                lifecycle.namespace_executions.get(incarnation, 0) + 1
            )
        try:
            index = self.runtime.index(namespace, record["incarnation"], generation)
            dense = building["manifest"]["index"]["dense"]
            dimension = int(dense["dimension"]) if dense else None
            recreated = not index.has_valid_collection(dense_dimension=dimension)
            if recreated:
                index.recreate(dense_dimension=dimension)
            else:
                index.load()
            with lifecycle.condition:
                current = lifecycle.namespaces.get(namespace, {})
                if current.get("building", {}).get("generation") != generation:
                    return
                updated = copy.deepcopy(current)
                updated["building"]["initialized"] = True
                updated["building"].pop("error", None)
                with lifecycle.state_transaction():
                    self.catalog.put_namespace(namespace, updated)
                    if recreated:
                        for identity, old in list(lifecycle.build_targets.items()):
                            if identity.namespace != namespace or old["stage"] == "process":
                                continue
                            target = dict(
                                old,
                                stage="chunk",
                                completed_batches=0,
                                plan=[],
                                state="cancelled"
                                if self.catalog.cancelled(namespace, identity.doc_id)
                                else "pending",
                            )
                            self.catalog.put_build(namespace, identity.doc_id, target)
                            lifecycle.build_targets[identity] = target
                lifecycle.namespaces[namespace] = updated
                lifecycle.condition.notify_all()
        finally:
            with lifecycle.condition:
                self.creating.discard(generation)
                lifecycle.namespace_executions[incarnation] -= 1
                lifecycle.condition.notify_all()

    def promote(self, namespace: str) -> None:
        lifecycle = self.lifecycle
        previous = lifecycle.namespaces.get(namespace, {})
        building = previous.get("building")
        if not building or not building["initialized"]:
            return
        if lifecycle.held(DocumentId(namespace, ""), previous):
            return
        generation = building["generation"]
        binding = self.runtime.build_bindings.get((namespace, generation))
        if binding is None:
            return
        members = {
            i: j
            for i, j in lifecycle.targets.items()
            if i.namespace == namespace and j["kind"] == "upsert"
        }
        if any(i.namespace == namespace and i not in members for i in lifecycle.build_targets):
            # Removed members still own private records and cleanup. Reconcile them
            # before publishing; a delayed maintenance pass cannot restore old intent.
            return
        for identity, desired in members.items():
            candidate = lifecycle.build_targets.get(identity, {})
            if candidate.get("source_revision") != desired["revision"]:
                return
            if candidate.get("state") == "succeeded":
                continue
            if (
                candidate.get("state") != "cancelled"
                or self.catalog.get_document(namespace, identity.doc_id) is not None
                or self.catalog.get_build(namespace, identity.doc_id, document=True) is not None
            ):
                return
            # A stopped input with no prepared text has no result to preserve in G0.
            # Carry its cancellation into G1 without blocking the usable members.
        if any(i.namespace == namespace for i, _ in lifecycle.executing):
            return
        updated = copy.deepcopy(previous)
        updated.pop("building")
        updated.update(
            manifest=building["manifest"],
            indexing=building["indexing"],
            binding=building["binding"],
            index_epoch=generation,
            active_generation=generation,
        )
        updated.setdefault("retiring_generations", []).append(previous.get("active_generation"))
        promoted: list[tuple[DocumentId, dict[str, Any]]] = []
        with lifecycle.state_transaction():
            self.catalog.put_namespace(namespace, updated)
            for identity in members:
                target = dict(lifecycle.build_targets[identity])
                target.pop("build_generation", None)
                target.pop("source_revision", None)
                prepared = self.catalog.get_build(namespace, identity.doc_id, document=True)
                if prepared is not None:
                    self.catalog.put_document(namespace, identity.doc_id, prepared)
                self.catalog.put_target(namespace, identity.doc_id, target)
                self.catalog.put_build(namespace, identity.doc_id, None, retire=False)
                self.catalog.put_build(namespace, identity.doc_id, None, document=True)
                promoted.append((identity, target))
        lifecycle.namespaces[namespace] = updated
        self.runtime.bindings[namespace] = binding
        self.runtime.build_bindings.pop((namespace, generation), None)
        self.runtime.index_errors.discard(namespace)
        for identity, target in promoted:
            lifecycle.build_targets.pop(identity, None)
            lifecycle.remember(identity, target)
        lifecycle.condition.notify_all()

    def retire(self, namespace: str) -> None:
        lifecycle = self.lifecycle
        with lifecycle.condition:
            record = lifecycle.namespaces.get(namespace)
            if record is None:
                return
            if lifecycle.held(DocumentId(namespace, ""), record):
                return
            if (
                record.get("retirement_failures", 0) >= 5
                or record.get("retirement_retry", 0) > time.time()
            ):
                return
            incarnation = record["incarnation"]
            generations = [
                g
                for g in record.get("retiring_generations", [])
                if (
                    not lifecycle.generation_queries.get((incarnation, g))
                    and not any(
                        j.get("incarnation") == incarnation and j.get("collection_generation") == g
                        for j in lifecycle.execution_records.values()
                    )
                    and g not in self.creating
                )
            ]
        for generation in generations:
            with lifecycle.condition:
                current = lifecycle.namespaces.get(namespace, {})
                if current.get("incarnation") != incarnation:
                    return
                if lifecycle.held(DocumentId(namespace, ""), current):
                    return
                lifecycle.namespace_executions[incarnation] = (
                    lifecycle.namespace_executions.get(incarnation, 0) + 1
                )
            try:
                index = self.runtime.index(namespace, incarnation, generation)
                if index.client.has_collection(index.collection_name):
                    index.drop()
                with lifecycle.condition:
                    current = lifecycle.namespaces.get(namespace, {})
                    if current.get("incarnation") != incarnation:
                        return
                    updated = copy.deepcopy(current)
                    updated["retiring_generations"].remove(generation)
                    updated.pop("retirement_error", None)
                    updated.pop("retirement_failures", None)
                    updated.pop("retirement_retry", None)
                    with lifecycle.state_transaction():
                        self.catalog.put_namespace(namespace, updated)
                    lifecycle.namespaces[namespace] = updated
                    self.runtime.build_bindings.pop((namespace, generation), None)
                    self.runtime.collections.pop(index.collection_name, None)
            finally:
                with lifecycle.condition:
                    lifecycle.namespace_executions[incarnation] -= 1
                    lifecycle.condition.notify_all()
