# pyright: reportPrivateUsage=false
from __future__ import annotations

import threading
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest
from test_review_boundaries import Model

from mfs import (
    MFS,
    DefaultChunker,
    DocumentId,
    GCPolicy,
    IgnoreRule,
    OperationFailed,
    SourceExcluded,
    StorageFailed,
    Utf8TextProcessor,
)


def test_status_page_checks_cleanup_without_decoding_the_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state", start_paused=True)) as mfs:
        for namespace in ("n", "other"):
            mfs.create_namespace(
                namespace, "internal", processors=[Utf8TextProcessor()], indexing="off"
            )
        reports = [mfs.upsert("n", f"{i}.txt", b"text") for i in range(40)]
        with mfs._condition, mfs._catalog.transaction():
            template = mfs._tasks.targets[reports[0].id]
            for i in range(500):
                mfs._catalog.enqueue_cleanup("n", "0.txt", dict(template, snapshot_id=str(i)))
            mfs._catalog.enqueue_cleanup("other", "1.txt", dict(template, snapshot_id="other"))

        def no_queue_read(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("status must not decode the cleanup queue")

        with monkeypatch.context() as patch:
            patch.setattr(mfs._catalog, "cleanup_rows", no_queue_read)
            page = mfs.list_document_statuses("n")
            assert len(page) == 40
            assert {s.id.doc_id for s in page if s.cleanup_pending} == {"0.txt"}
            mfs.cancel(reports[0].id)
            assert mfs.document_status(reports[0].id).state == "cancelled"  # type: ignore[union-attr]


def test_claim_failure_backoff_survives_notifications_and_reopen_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state"
    attempts: list[float] = []
    stopped = threading.Event()
    thread: threading.Thread | None = None
    mfs = MFS.open(state, gc_policy=GCPolicy(enabled=False))
    try:
        mfs.create_namespace(
            "n",
            "internal",
            processors=[Utf8TextProcessor()],
            indexing="off",
            processing_paused=True,
        )
        report = mfs.upsert("n", "a.txt", b"durably accepted input")
        original = mfs._catalog.put_active

        def fail(namespace: str, doc_id: str, value: Any) -> None:
            if value is not None:
                attempts.append(time.monotonic())
                raise StorageFailed("claim write unavailable")
            original(namespace, doc_id, value)

        def notifications() -> None:
            while not stopped.wait(0.001):
                with mfs._condition:
                    mfs._condition.notify_all()

        thread = threading.Thread(target=notifications)
        with monkeypatch.context() as patch:
            patch.setattr(mfs._catalog, "put_active", fail)
            thread.start()
            mfs.configure_processing("n", paused=False)
            with pytest.raises(StorageFailed, match="claim write unavailable"):
                mfs.wait(report, 5)
            assert len(attempts) == 3
            assert attempts[1] - attempts[0] >= 0.24
            assert attempts[2] - attempts[1] >= 0.49
            assert mfs.status().index_state == "dirty"
            status = mfs.document_status(report.id)
            assert status is not None and status.error is not None
            assert "claim write unavailable" in status.error
    finally:
        stopped.set()
        if thread is not None:
            thread.join(5)
        mfs.close(timeout=10)
    with closing(MFS.open(state)) as recovered:
        recovered.open_namespace("n", processors=[Utf8TextProcessor()])
        recovered.wait("n", 10)
        assert recovered.read(DocumentId("n", "a.txt")) is not None


def test_claim_adopts_a_committed_claim_when_acknowledgement_is_lost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n",
            "internal",
            processors=[Utf8TextProcessor()],
            indexing="off",
            processing_paused=True,
        )
        report = mfs.upsert("n", "a.txt", b"needle")
        original = mfs._catalog.transaction
        armed = True

        @contextmanager
        def lose_ack() -> Generator[None]:
            nonlocal armed
            absent = mfs._catalog.one("SELECT 1 FROM active_runs") is None
            with original():
                yield
            if armed and absent and mfs._catalog.one("SELECT 1 FROM active_runs") is not None:
                armed = False
                raise StorageFailed("lost claim acknowledgement")

        monkeypatch.setattr(mfs._catalog, "transaction", lose_ack)
        mfs.configure_processing("n", paused=False)
        mfs.wait(report, 10)
        assert not armed
        assert mfs._tasks.claim_failures == 0
        assert mfs._tasks.storage_error is None


def test_transient_claim_failure_recovers_without_reopening(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n",
            "internal",
            processors=[Utf8TextProcessor()],
            indexing="off",
            processing_paused=True,
        )
        report = mfs.upsert("n", "a.txt", b"accepted input")
        original = mfs._catalog.put_active
        failures = 0

        def fail_once(namespace: str, doc_id: str, value: Any) -> None:
            nonlocal failures
            if value is not None and failures == 0:
                failures += 1
                raise StorageFailed("temporary claim write failure")
            original(namespace, doc_id, value)

        monkeypatch.setattr(mfs._catalog, "put_active", fail_once)
        mfs.configure_processing("n", paused=False)
        mfs.wait(report, 10)
        assert failures == 1
        assert mfs._tasks.claim_failures == 0
        assert mfs._tasks.storage_error is None


def test_internal_accept_rechecks_rules_after_preparing_the_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        original = mfs._prepare_original

        def pause(staged: Any, incarnation: str) -> None:
            original(staged, incarnation)
            entered.set()
            assert release.wait(10)

        monkeypatch.setattr(mfs, "_prepare_original", pause)
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(mfs.upsert, "n", "a.txt", b"excluded while preparing")
            try:
                assert entered.wait(5)
                mfs.update_rules(
                    "n",
                    expected_revision=mfs.rules("n").revision,
                    add=[IgnoreRule("hide", "a.txt")],
                )
            finally:
                release.set()
            with pytest.raises(SourceExcluded):
                future.result(5)
        assert mfs.document_status(DocumentId("n", "a.txt")) is None
        mfs.wait("n", 5)


def test_reopen_retires_a_legacy_target_accepted_after_its_exclusion(tmp_path: Path) -> None:
    state = tmp_path / "state"
    with closing(MFS.open(state, start_paused=True)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        report = mfs.upsert("n", "a.txt", b"legacy invalid target")
        # Reproduce the durable state written by the old accept/rules race.
        with mfs._condition, mfs._catalog.transaction():
            record = dict(mfs._tasks.namespaces["n"], rules=[asdict(IgnoreRule("hide", "a.txt"))])
            mfs._catalog.put_namespace("n", record)
            job = dict(mfs._tasks.targets[report.id], state="running")
            mfs._catalog.put_target("n", "a.txt", job)
    with closing(MFS.open(state)) as mfs:
        mfs.open_namespace("n", processors=[Utf8TextProcessor()])
        mfs.wait("n", 10)
        assert mfs._catalog.get_target("n", "a.txt")["kind"] == "delete"  # type: ignore[index]
        assert mfs.read(report.id) is None
        assert not mfs.grep("n").items


def test_rules_survive_candidate_cleanup_failure_without_old_target_resurrection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n",
            "internal",
            processors=[Utf8TextProcessor()],
            indexing="off",
            processing_paused=True,
        )
        reports = [mfs.upsert("n", name, b"hello") for name in ("a.txt", "b.txt")]
        chunker = DefaultChunker()
        chunker.version = "2"
        mfs.configure_namespace("n", chunker=chunker)
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: len(mfs._tasks.build_targets) == 2, 10)
        failed = threading.Event()
        original = mfs._catalog.put_build

        def fail_once(namespace: str, doc_id: str, value: Any, **kwargs: Any) -> None:
            if value is None and not kwargs.get("document") and not failed.is_set():
                failed.set()
                raise StorageFailed("candidate member deletion unavailable")
            original(namespace, doc_id, value, **kwargs)

        monkeypatch.setattr(mfs._catalog, "put_build", fail_once)
        mfs.update_rules(
            "n", expected_revision=mfs.rules("n").revision, add=[IgnoreRule("hide", "*.txt")]
        )
        assert failed.wait(5)
        with mfs._condition:
            for report in reports:
                assert mfs._tasks.targets[report.id]["kind"] == "delete"
                assert mfs._catalog.get_target("n", report.id.doc_id)["kind"] == "delete"  # type: ignore[index]
        mfs.configure_processing("n", paused=False)
        mfs.wait("n", 10)
        assert not mfs._tasks.build_targets
        for report in reports:
            assert mfs._catalog.get_target("n", report.id.doc_id)["kind"] == "delete"  # type: ignore[index]
            assert mfs.read(report.id) is None


def test_cancelled_unprepared_input_does_not_block_new_index_capability(tmp_path: Path) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n",
            "internal",
            processors=[Utf8TextProcessor()],
            indexing="bm25",
            processing_paused=True,
        )
        cancelled = mfs.upsert("n", "cancelled.txt", b"cancelled input")
        healthy = mfs.upsert("n", "healthy.txt", b"healthy searchable text")
        mfs.cancel(cancelled.id)
        mfs.configure_processing("n", paused=False)
        mfs.wait(healthy, 10)
        change = mfs.configure_namespace("n", embedder=Model("new"), indexing="hybrid")
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs.namespace_configuration("n").active_revision == change.revision, 10
            )
        assert mfs.search("n", "healthy", mode="vector").items
        status = mfs.document_status(cancelled.id)
        assert status is not None and status.state == "cancelled"
        assert mfs._catalog.cancelled("n", "cancelled.txt")
        with pytest.raises(OperationFailed):
            mfs.wait(cancelled, 1)
        mfs.retry(cancelled.id)
        mfs.wait(cancelled, 10)


def test_cancelled_prepared_member_keeps_serving_configuration(tmp_path: Path) -> None:
    old, new = Model("old"), Model("new")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=old)
        report = mfs.upsert("n", "a.txt", b"old searchable text")
        mfs.wait(report, 10)
        previous = mfs.namespace_configuration("n").active_revision
        new.release.clear()
        change = mfs.configure_namespace("n", embedder=new)
        try:
            assert new.entered.wait(5)
            mfs.cancel(report.id)
        finally:
            new.release.set()
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: not mfs._tasks.executing, 10)
            assert mfs.namespace_configuration("n").active_revision == previous
            assert mfs.namespace_configuration("n").pending_revision == change.revision
        with pytest.raises(OperationFailed):
            mfs.wait(change, 1)
        assert mfs.search("n", "old", mode="vector").items


def test_status_cancel_and_retry_use_new_input_before_candidate_reconciliation(
    tmp_path: Path,
) -> None:
    entered, release = threading.Event(), threading.Event()

    class SlowOther(Model):
        def embed_documents(self, texts: Any) -> list[list[float]]:
            if any("hold" in text for text in texts):
                entered.set()
                assert release.wait(15)
            return [[1.0, 0.0] for _ in texts]

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], embedder=Model("old")
        )
        a = mfs.upsert("n", "a.txt", b"first")
        mfs.upsert("n", "b.txt", b"hold")
        mfs.wait("n", 10)
        mfs.configure_namespace("n", embedder=SlowOther("new"))
        try:
            assert entered.wait(5)
            with mfs._condition:
                assert mfs._condition.wait_for(
                    lambda: mfs._tasks.build_targets.get(a.id, {}).get("state") == "succeeded", 10
                )
                replacement = mfs.upsert("n", "a.txt", b"replacement")
                status = mfs.document_status(a.id)
                assert status is not None and status.revision == replacement.revision
                mfs.cancel(a.id)
                assert mfs._catalog.cancelled("n", "a.txt")
                status = mfs.document_status(a.id)
                assert status is not None and status.state == "cancelled"
                mfs.retry(a.id)
                assert not mfs._catalog.cancelled("n", "a.txt")
                assert mfs._tasks.targets[a.id]["state"] == "pending"
        finally:
            release.set()
        mfs.wait("n", 10)


def test_rule_cache_refresh_does_not_write_active_run_retirement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state", start_paused=True)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        reports = [mfs.upsert("n", name, b"text") for name in ("a.txt", "b.txt")]
        with mfs._condition:
            # A stage checkpoint has persisted its next target and active run;
            # the prior invocation exited and no thread still owns the permit.
            with mfs._catalog.transaction():
                for report in reports:
                    job = dict(mfs._tasks.targets[report.id], active_run_id=report.id.doc_id)
                    mfs._catalog.put_target("n", report.id.doc_id, job)
                    mfs._catalog.put_active("n", report.id.doc_id, job)
                    mfs._tasks.remember(report.id, job)
                    mfs._tasks.active[report.id] = job
            original = mfs._catalog.put_active

            def fail_retirement(namespace: str, doc_id: str, value: Any) -> None:
                if value is None:
                    raise StorageFailed("active retirement unavailable")
                original(namespace, doc_id, value)

            with monkeypatch.context() as patch:
                patch.setattr(mfs._catalog, "put_active", fail_retirement)
                mfs._tasks.boot_paused = False
                mfs.update_rules(
                    "n",
                    expected_revision=mfs.rules("n").revision,
                    add=[IgnoreRule("hide", "*.txt")],
                )
                assert all(mfs._tasks.targets[r.id]["kind"] == "delete" for r in reports)
        mfs.wait("n", 10)


def test_claim_discards_cached_work_missing_from_sqlite(tmp_path: Path) -> None:
    with closing(MFS.open(tmp_path / "state", start_paused=True)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        report = mfs.upsert("n", "a.txt", b"old cache entry")
        # Model an acknowledged durable removal with an obsolete runnable cache.
        with mfs._condition, mfs._catalog.transaction():
            mfs._catalog.delete_targets("n")
        mfs.resume_background()
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: report.id not in mfs._tasks.targets, 5)
        mfs.wait("n", 5)


def test_candidate_deletion_lost_ack_and_failed_read_recover_from_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n",
            "internal",
            processors=[Utf8TextProcessor()],
            indexing="off",
            processing_paused=True,
        )
        report = mfs.upsert("n", "a.txt", b"text")
        chunker = DefaultChunker()
        chunker.version = "2"
        mfs.configure_namespace("n", chunker=chunker)
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: report.id in mfs._tasks.build_targets, 10)
        original_transaction, original_get = mfs._catalog.transaction, mfs._catalog.get_build
        armed, fail_read = True, False
        observed = threading.Event()

        @contextmanager
        def lose_ack() -> Generator[None]:
            nonlocal armed, fail_read
            before = mfs._catalog.one("SELECT 1 FROM build_targets")
            with original_transaction():
                yield
            if armed and before and mfs._catalog.one("SELECT 1 FROM build_targets") is None:
                armed, fail_read = False, True
                raise StorageFailed("lost candidate deletion acknowledgement")

        def get_build(namespace: str, doc_id: str, *, document: bool = False) -> Any:
            nonlocal fail_read
            if fail_read:
                fail_read = False
                observed.set()
                raise StorageFailed("candidate reconciliation read unavailable")
            return original_get(namespace, doc_id, document=document)

        monkeypatch.setattr(mfs._catalog, "transaction", lose_ack)
        monkeypatch.setattr(mfs._catalog, "get_build", get_build)
        mfs.update_rules(
            "n", expected_revision=mfs.rules("n").revision, add=[IgnoreRule("hide", "a.txt")]
        )
        assert observed.wait(5)
        mfs.wait("n", 10)
        assert not mfs._tasks.build_targets
        assert mfs._tasks.storage_error is None
