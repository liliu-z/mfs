from __future__ import annotations

import json
import copy
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from pathlib import Path

from mfs import MFS, DefaultChunker, DocumentId, ExecutionPolicy, GCPolicy, IgnoreRule, Utf8TextProcessor, StorageFailed


def outcome(call):
    try:
        return repr(call())
    except Exception as error:
        return f"{type(error).__name__}: {error}"


def rules_during_upsert(base):
    m = MFS.open(base / "rule-race", gc_policy=GCPolicy(enabled=False),
                 execution=ExecutionPolicy(stage_timeout=0.2))
    arrived, release = threading.Event(), threading.Event()
    original = m._prepare_original

    def pause(staged, incarnation):
        original(staged, incarnation)
        arrived.set()
        assert release.wait(5)

    try:
        m.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        m._prepare_original = pause
        with ThreadPoolExecutor() as pool:
            pending = pool.submit(m.upsert, "n", "secret.txt", b"secret bytes")
            assert arrived.wait(5)
            m.update_rules("n", expected_revision=m.rules("n").revision,
                           add=[IgnoreRule("excluded", "secret.txt")])
            release.set()
            result = outcome(lambda: pending.result(5))
        time.sleep(0.5)
        status = m.document_status(DocumentId("n", "secret.txt"))
        print(json.dumps({"probe": "rule-race", "upsert": result,
                          "state": status.state if status else None,
                          "executing": status.executing if status else None,
                          "wait": outcome(lambda: m.wait("n", 0.1)),
                          "retry": outcome(lambda: m.retry(DocumentId("n", "secret.txt"))),
                          "grep": str(m.grep("n").items)}, ensure_ascii=False), flush=True)
    finally:
        release.set()
        m.close(timeout=5)
    recovered = MFS.open(base / "rule-race", gc_policy=GCPolicy(enabled=False),
                         execution=ExecutionPolicy(stage_timeout=0.2))
    try:
        recovered.open_namespace("n", processors=[Utf8TextProcessor()])
        time.sleep(0.5)
        current = recovered.document_status(DocumentId("n", "secret.txt"))
        print(json.dumps({"probe": "rule-race-reopen", "state": current.state if current else None,
                          "executing": current.executing if current else None,
                          "wait": outcome(lambda: recovered.wait("n", 0.1)),
                          "retry": outcome(lambda: recovered.retry(DocumentId("n", "secret.txt")))}), flush=True)
    finally:
        recovered.close(timeout=5)


def rules_postcommit_failure(base):
    m = MFS.open(base / "rule-postcommit", gc_policy=GCPolicy(enabled=False))
    original = m._catalog.put_build
    try:
        m.create_namespace("n", "internal", processors=[Utf8TextProcessor()],
                           indexing="off", processing_paused=True)
        reports = [m.upsert("n", name, b"hello") for name in ("a.txt", "b.txt")]
        chunker = DefaultChunker()
        chunker.version = "review-2"
        m.configure_namespace("n", chunker=chunker)
        with m._condition:
            assert m._condition.wait_for(lambda: len(m._tasks.build_targets) == 2, 10)
            armed = True

            def fail(namespace, doc_id, value, **kwargs):
                nonlocal armed
                if armed and value is None and not kwargs.get("document"):
                    armed = False
                    raise StorageFailed("injected member deletion failure after rule commit")
                return original(namespace, doc_id, value, **kwargs)

            m._catalog.put_build = fail
            result = outcome(lambda: m.update_rules(
                "n", expected_revision=m.rules("n").revision,
                add=[IgnoreRule("hide", "*.txt")],
            ))
            m._catalog.put_build = original
            print(json.dumps({"probe": "rules-postcommit-memory", "update": result,
                              "targets": {r.id.doc_id: {
                                  "sqlite": m._catalog.get_target("n", r.id.doc_id)["kind"],
                                  "memory": m._tasks.targets[r.id]["kind"],
                              } for r in reports}}), flush=True)
        m.configure_processing("n", paused=False)
        result = outcome(lambda: m.wait("n", 5))
        with m._condition:
            print(json.dumps({"probe": "rules-postcommit-failure-cleared", "wait": result,
                              "targets": {r.id.doc_id: {
                                  "sqlite": m._catalog.get_target("n", r.id.doc_id)["kind"],
                                  "memory": m._tasks.targets[r.id]["kind"],
                                  "state": m.document_status(r.id).state,
                                  "executing": m.document_status(r.id).executing,
                              } for r in reports}}), flush=True)
    finally:
        m._catalog.put_build = original
        m.close(timeout=10)


def claim_storage_failure(base):
    m = MFS.open(base / "claim-failure", gc_policy=GCPolicy(enabled=False),
                 execution=ExecutionPolicy(stage_timeout=0.2))
    original = m._catalog.put_active
    attempts = 0

    def fail(namespace, doc_id, value):
        nonlocal attempts
        if value is not None:
            attempts += 1
            raise StorageFailed("injected durable claim failure")
        return original(namespace, doc_id, value)

    try:
        m.create_namespace("n", "internal", processors=[Utf8TextProcessor()],
                           indexing="off", processing_paused=True)
        report = m.upsert("n", "a.txt", b"input already durably accepted")
        m._catalog.put_active = fail
        m.configure_processing("n", paused=False)
        time.sleep(2)
        current = m.document_status(report.id)
        print(json.dumps({"probe": "claim-failure", "claim_failures": attempts,
                          "state": current.state, "error": current.error,
                          "storage_error": str(m._tasks.storage_error),
                          "wait": outcome(lambda: m.wait("n", 0.1))}), flush=True)
    finally:
        m._catalog.put_active = original
        m.close(timeout=5)
    recovered = MFS.open(base / "claim-failure", gc_policy=GCPolicy(enabled=False))
    try:
        recovered.open_namespace("n", processors=[Utf8TextProcessor()])
        recovered.wait("n", 10)
        print(json.dumps({"probe": "claim-failure-cleared-reopen",
                          "state": recovered.document_status(report.id).state}), flush=True)
    finally:
        recovered.close(timeout=5)


class Model:
    id = "review-model"
    version = "1"
    options = {}
    embedding_space = "review-space"
    dimension = 2

    def embed_documents(self, texts):
        return [[1.0, 0.0] for _ in texts]

    def embed_query(self, text):
        return [1.0, 0.0]


def cancelled_member_blocks_upgrade(base):
    m = MFS.open(base / "cancelled-member", gc_policy=GCPolicy(enabled=False))
    try:
        m.create_namespace("n", "internal", processors=[Utf8TextProcessor()],
                           indexing="bm25", processing_paused=True)
        cancelled = m.upsert("n", "cancelled.txt", b"user does not want this processed")
        healthy = m.upsert("n", "healthy.txt", b"ordinary healthy searchable file")
        m.cancel(cancelled.id)
        m.configure_processing("n", paused=False)
        m.wait(healthy, 10)
        accepted = m.configure_namespace("n", embedder=Model(), indexing="hybrid")
        deadline = time.monotonic() + 10
        while (m.document_status(healthy.id).configuration_revision != accepted.revision
               or m.document_status(healthy.id).state != "succeeded"):
            assert time.monotonic() < deadline
            time.sleep(0.02)
        time.sleep(0.5)
        print(json.dumps({"probe": "cancelled-member-upgrade",
                          "healthy_state": m.document_status(healthy.id).state,
                          "cancelled_state": m.document_status(cancelled.id).state,
                          "pending_revision": m.namespace_configuration("n").pending_revision,
                          "mode": m.namespace_configuration("n").indexing,
                          "wait": outcome(lambda: m.wait(accepted, 0.1)),
                          "vector_search": outcome(lambda: m.search("n", "healthy", mode="vector"))}), flush=True)
    finally:
        m.close(timeout=10)


def status_cleanup_scaling(base):
    m = MFS.open(base / "status-debts", start_paused=True,
                 gc_policy=GCPolicy(enabled=False))
    try:
        m.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        seed = m.upsert("n", "doc-000.txt", b"synthetic status fixture")
        with m._condition, m._catalog.transaction():
            template = m._tasks.targets[seed.id]
            for i in range(1, 250):
                identity = DocumentId("n", f"doc-{i:03d}.txt")
                job = copy.deepcopy(template)
                job["identity"] = {"namespace": "n", "doc_id": identity.doc_id}
                job["revision"] = f"{i:032x}"
                m._catalog.put_target("n", identity.doc_id, job)
                m._tasks.remember(identity, job)
            for i in range(3000):
                debt_job = dict(template, snapshot_id=f"{i:064x}")
                m._catalog.enqueue_cleanup("n", f"doc-{i % 250:03d}.txt", debt_job)
        queries = 0
        entered = threading.Event()

        def count(sql):
            nonlocal queries
            if sql.startswith("SELECT key,value FROM index_cleanup"):
                queries += 1
            if sql.startswith("SELECT") and "FROM index_cleanup" in sql:
                entered.set()

        m._catalog.set_trace_callback(count)
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=2) as pool:
            listing = pool.submit(m.list_document_statuses, "n", limit=250)
            assert entered.wait(5)
            control = pool.submit(m.cancel, seed.id)
            try:
                control.result(0.2)
                control_blocked = False
            except FutureTimeout:
                control_blocked = True
            statuses = listing.result(45)
            control.result(5)
        elapsed = time.monotonic() - started
        print(json.dumps({"probe": "status-cleanup-scaling", "documents": len(statuses),
                          "cleanup_debts": 3000, "full_debt_queries": queries,
                          "cancel_blocked_after_200ms": control_blocked,
                          "seconds_holding_lifecycle_lock": round(elapsed, 3)}), flush=True)
    finally:
        m.close(timeout=10)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="mfs-review-") as directory:
        rules_during_upsert(Path(directory))
        rules_postcommit_failure(Path(directory))
        claim_storage_failure(Path(directory))
        cancelled_member_blocks_upgrade(Path(directory))
        status_cleanup_scaling(Path(directory))
