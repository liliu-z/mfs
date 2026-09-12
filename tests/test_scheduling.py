# pyright: reportPrivateUsage=false
from __future__ import annotations

import threading
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from test_extensions import ContextAdapter
from test_lifecycle import GateEmbedder
from test_namespace_contract import Model
from test_search_timeout import SlowQuery

from mfs import (
    MFS,
    DocumentId,
    GCPolicy,
    OperationFailed,
    ProcessedDocument,
    ProcessingContext,
    RetryableError,
    SourceMap,
    StorageFailed,
    UnderPath,
    Utf8TextProcessor,
    WaitTimeout,
)
from mfs.processing import _ProcessingStopped, _ProcessingYielded


@pytest.mark.parametrize("action", ["replace", "cancel_retry", "drop", "reindex", "close"])
def test_yield_keeps_execution_and_files_until_stack_exits(tmp_path: Path, action: str) -> None:
    entered, checkpoint, retiring, release = (threading.Event() for _ in range(4))
    contexts: list[ProcessingContext] = []
    active = maximum = 0

    class Adapter(ContextAdapter):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext
        ) -> ProcessedDocument:
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            contexts.append(context)
            try:
                if len(contexts) == 1:
                    entered.set()
                    assert checkpoint.wait(10)
                    part = context.work_dir / "part"
                    part.write_text("saved")
                    try:
                        context.checkpoint({"unit": 1}, files={"part": part})
                    finally:
                        retiring.set()
                        assert release.wait(10)
                if context.resume_state:
                    assert context.resume_files["part"].read_text() == "saved"
                return ProcessedDocument(staged_path.read_text(), SourceMap(1, ()))
            finally:
                active -= 1

    state = tmp_path / "state"
    adapter = Adapter()
    mfs = MFS.open(state, gc_policy=GCPolicy(idle_seconds=0, grace_seconds=0))
    bg = DocumentId("n", "background.txt")
    with ThreadPoolExecutor() as pool:
        try:
            mfs.create_namespace("n", "internal", processors=[adapter])
            first = mfs.upsert("n", bg.doc_id, b"original")
            assert entered.wait(5)
            mfs.set_active_scopes([UnderPath("n", "active.txt")])
            mfs.upsert("n", "active.txt", b"active")
            checkpoint.set()
            assert retiring.wait(5)
            assert len(contexts) == 1 and mfs.collect_garbage().busy
            status = mfs.document_status(bg)
            assert status and status.state == "running" and status.revision == first.revision
            future = None
            if action == "replace":
                mfs.upsert("n", bg.doc_id, b"replacement")
            elif action == "cancel_retry":
                mfs.cancel(bg)
                mfs.retry(bg)
            elif action == "drop":
                mfs.drop_namespace("n")
                mfs.create_namespace("n", "internal", processors=[adapter])
                mfs.upsert("n", bg.doc_id, b"replacement")
            elif action == "reindex":
                future = pool.submit(mfs.reindex, "n", timeout=10)
                with mfs._condition:
                    assert mfs._condition.wait_for(
                        lambda: "pending_manifest" in mfs._tasks.namespaces["n"], 5
                    )
            else:
                future = pool.submit(mfs.close)
                with mfs._condition:
                    assert mfs._condition.wait_for(lambda: mfs._tasks.stopping, 5)
            assert len(contexts) == 1  # New intent is accepted; no second execution yet.
            release.set()
            if future is not None:
                future.result(10)
            if action == "close":
                mfs = MFS.open(state)
                mfs.open_namespace("n", processors=[adapter])
            mfs.wait(bg, 10)
            mfs.wait_ready(10)
            assert maximum == 1
            document = mfs.read(bg)
            assert document and document.text == (
                "replacement" if action in ("replace", "drop") else "original"
            )
            resumed = [c for c in contexts[1:] if c.document_id == bg]
            assert len(resumed) == 1
            assert bool(resumed[0].resume_state) == (action not in ("replace", "drop"))
            before = mfs.document_status(bg)
            with pytest.raises((_ProcessingStopped, _ProcessingYielded)):
                contexts[0].report_progress(99)
            assert mfs.document_status(bg) == before
        finally:
            checkpoint.set()
            release.set()
            mfs.close()


@pytest.mark.parametrize("cleanup", ["return", "error"])
def test_finally_cannot_publish_after_yield_and_errors_keep_checkpoint(
    tmp_path: Path, cleanup: str
) -> None:
    entered, checkpoint, active, release = (threading.Event() for _ in range(4))

    class Adapter(ContextAdapter):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext
        ) -> ProcessedDocument:
            if context.document_id.doc_id == "active.txt":
                active.set()
                assert release.wait(10)
            elif context.resume_state is None:
                entered.set()
                assert checkpoint.wait(10)
                try:
                    context.checkpoint({"unit": 1})
                finally:
                    if cleanup == "error":
                        raise ValueError("cleanup failed")
                    return ProcessedDocument("partial must never publish", SourceMap(1, ()))  # noqa: B012
            return ProcessedDocument("complete", SourceMap(1, ()))

    mfs = MFS.open(tmp_path / "state")
    bg = DocumentId("n", "background.txt")
    try:
        mfs.create_namespace("n", "internal", processors=[Adapter()])
        mfs.upsert("n", bg.doc_id, b"source")
        assert entered.wait(5)
        mfs.set_active_scopes([UnderPath("n", "active.txt")])
        mfs.upsert("n", "active.txt", b"source")
        checkpoint.set()
        assert active.wait(5)
        assert mfs.read(bg) is None
        assert mfs._tasks.targets[bg]["checkpoint"]["state"] == {"unit": 1}
        if cleanup == "error":
            with pytest.raises(OperationFailed, match="cleanup failed"):
                mfs.wait(bg, 0)
            assert mfs._tasks.targets[bg]["failures"] == 1
            mfs.retry(bg)
        release.set()
        mfs.wait(bg, 10)
        document = mfs.read(bg)
        assert document and document.text == "complete"
    finally:
        checkpoint.set()
        release.set()
        mfs.close()


@pytest.mark.parametrize("control", ["reindex", "drop"])
def test_timed_out_query_pins_collection_before_backend_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, control: str
) -> None:
    model = SlowQuery()
    mfs = MFS.open(tmp_path / "state")
    destroyed = threading.Event()
    with ThreadPoolExecutor() as pool:
        try:
            mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=model)
            mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
            index = mfs._runtime.index("n")
            drop = index.drop

            def tracked_drop() -> None:
                destroyed.set()
                drop()

            monkeypatch.setattr(index, "drop", tracked_drop)
            recreate = index.recreate

            def tracked_recreate(*, dense_dimension: int | None) -> None:
                destroyed.set()
                recreate(dense_dimension=dense_dimension)

            monkeypatch.setattr(index, "recreate", tracked_recreate)
            query = pool.submit(mfs.search, "n", "needle", mode="vector", timeout=0.1)
            model.wait_queries(1)
            with pytest.raises(WaitTimeout):
                query.result(2)
            if control == "reindex":
                future = pool.submit(mfs.reindex, "n", timeout=10)
                with mfs._condition:
                    assert mfs._condition.wait_for(
                        lambda: "pending_manifest" in mfs._tasks.namespaces["n"], 5
                    )
            else:
                report = mfs.drop_namespace("n")
                future = pool.submit(mfs.wait, report, 10)
                mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
                mfs.upsert("n", "new.txt", b"new needle")
            assert not destroyed.is_set()
            assert mfs._tasks.queries
            model.query_release.set()
            future.result(10)
            mfs.wait_ready(10)
            assert destroyed.is_set()
            assert mfs.search("n", "needle", mode="bm25").items
        finally:
            model.query_release.set()
            mfs.close()


def test_checkpoint_lost_ack_is_adopted_before_yield(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    ack_lost = False

    class Adapter(ContextAdapter):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext
        ) -> ProcessedDocument:
            if context.document_id.doc_id == "background.txt" and context.resume_state is None:
                entered.set()
                assert release.wait(10)
                context.checkpoint({"unit": 1})
            return ProcessedDocument("complete", SourceMap(1, ()))

    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Adapter()])
        bg = mfs.upsert("n", "background.txt", b"source").id
        assert entered.wait(5)
        original = mfs._catalog.transaction

        @contextmanager
        def lost_ack() -> Generator[None]:
            nonlocal ack_lost
            with original():
                yield
            if threading.current_thread().name == "mfs-worker" and not ack_lost:
                durable = mfs._catalog.get_target("n", "background.txt")
                if durable and durable.get("checkpoint", {}).get("state") == {"unit": 1}:
                    ack_lost = True
                    raise OSError("committed checkpoint acknowledgement lost")

        monkeypatch.setattr(mfs._catalog, "transaction", lost_ack)
        mfs.set_active_scopes([UnderPath("n", "active.txt")])
        mfs.upsert("n", "active.txt", b"source")
        release.set()
        mfs.wait(bg, 10)
        assert ack_lost and mfs._tasks.targets[bg]["failures"] == 0
        assert mfs.read(bg) is not None
    finally:
        release.set()
        mfs.close()


@pytest.mark.parametrize("action", ["replace", "reindex", "drop"])
def test_late_insert_is_retired_before_new_index_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    entered, release = threading.Event(), threading.Event()
    model = GateEmbedder()
    mfs = MFS.open(tmp_path / "state")
    with ThreadPoolExecutor() as pool:
        try:
            mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=model)
            index = mfs._runtime.index("n")
            insert = index.insert
            first = True

            def late_insert(rows: Any) -> None:
                nonlocal first
                if first:
                    first = False
                    entered.set()
                    assert release.wait(10)
                insert(rows)

            monkeypatch.setattr(index, "insert", late_insert)
            bg = mfs.upsert("n", "a.txt", b"old needle").id
            assert entered.wait(5)
            future = None
            if action == "reindex":
                future = pool.submit(mfs.reindex, "n", embedder=Model(3, "new-space"), timeout=10)
                with mfs._condition:
                    assert mfs._condition.wait_for(
                        lambda: "pending_manifest" in mfs._tasks.namespaces["n"], 5
                    )
            else:
                if action == "drop":
                    mfs.drop_namespace("n")
                    mfs.create_namespace(
                        "n", "internal", processors=[Utf8TextProcessor()], embedder=model
                    )
                mfs.upsert("n", "a.txt", b"new needle")
            release.set()
            if future is not None:
                future.result(10)
            mfs.wait(bg, 10)
            hits = mfs.search("n", "needle", mode="vector").items
            assert len(hits) == 1
            assert hits[0].value.text == ("old needle" if action == "reindex" else "new needle")
            assert mfs._runtime.index("n").count_document(bg) == 1
        finally:
            release.set()
            mfs.close()


def test_yield_does_not_reset_prior_failures(tmp_path: Path) -> None:
    entered, checkpoint, active, release = (threading.Event() for _ in range(4))
    calls = 0

    class Adapter(ContextAdapter):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext
        ) -> ProcessedDocument:
            nonlocal calls
            if context.document_id.doc_id == "active.txt":
                active.set()
                assert release.wait(10)
                return ProcessedDocument("active", SourceMap(1, ()))
            calls += 1
            if calls == 2:
                entered.set()
                assert checkpoint.wait(10)
                context.checkpoint({"unit": 1})
            raise RetryableError("keep the original failure budget")

    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Adapter()])
        bg = mfs.upsert("n", "background.txt", b"source").id
        assert entered.wait(5)
        mfs.set_active_scopes([UnderPath("n", "active.txt")])
        mfs.upsert("n", "active.txt", b"source")
        checkpoint.set()
        assert active.wait(5)
        assert mfs._tasks.targets[bg]["failures"] == 1
        assert mfs._tasks.targets[bg]["checkpoint"]["state"] == {"unit": 1}
        release.set()
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: mfs._tasks.targets[bg]["state"] == "failed", 10)
        assert calls == 6 and mfs._tasks.targets[bg]["failures"] == 5
    finally:
        checkpoint.set()
        release.set()
        mfs.close()


def test_paused_index_cannot_cause_checkpoint_yield(tmp_path: Path) -> None:
    calls = 0

    class Adapter(ContextAdapter):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext
        ) -> ProcessedDocument:
            nonlocal calls
            calls += 1
            context.checkpoint({"unit": 1})
            return ProcessedDocument("background", SourceMap(1, ()))

    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("active", "internal", processors=[Utf8TextProcessor()])
        mfs.configure_index("active", paused=True)
        urgent = mfs.upsert("active", "a.txt", b"waiting only for indexing").id
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs._tasks.targets[urgent]["stage"] == "chunk", 5
            )
        mfs.set_active_scopes([UnderPath("active")])
        mfs.create_namespace("bg", "internal", processors=[Adapter()])
        mfs.wait(mfs.upsert("bg", "b.txt", b"background"), 10)
        assert calls == 1
    finally:
        mfs.close()


def test_rebuild_supersession_and_lost_ack_activate_only_latest_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    mfs = MFS.open(tmp_path / "state")
    old, intermediate, newest = Model(2, "old"), Model(3, "intermediate"), Model(4, "newest")
    ack_lost = False
    with ThreadPoolExecutor() as pool:
        try:
            mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=old)
            mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
            index = mfs._runtime.index("n")
            recreate, transaction = index.recreate, mfs._catalog.transaction
            dimensions: list[int | None] = []

            def blocked_recreate(*, dense_dimension: int | None) -> None:
                dimensions.append(dense_dimension)
                if len(dimensions) == 1:
                    entered.set()
                    assert release.wait(10)
                recreate(dense_dimension=dense_dimension)

            @contextmanager
            def lost_ack() -> Generator[None]:
                nonlocal ack_lost
                with transaction():
                    yield
                if threading.current_thread().name == "mfs-worker" and not ack_lost:
                    record = mfs._catalog.get_namespace("n")
                    if record and record["manifest"]["index"]["dense"]["dimension"] == 4:
                        ack_lost = True
                        raise OSError("rebuild committed, acknowledgement lost")

            monkeypatch.setattr(index, "recreate", blocked_recreate)
            monkeypatch.setattr(mfs._catalog, "transaction", lost_ack)
            first = pool.submit(mfs.reindex, "n", embedder=intermediate, timeout=10)
            assert entered.wait(5)
            second = pool.submit(mfs.reindex, "n", embedder=newest, timeout=10)
            with mfs._condition:
                assert mfs._condition.wait_for(
                    lambda: (
                        mfs._tasks.namespaces["n"]["pending_manifest"]["index"]["dense"][
                            "dimension"
                        ]
                        == 4
                    ),
                    5,
                )
            release.set()
            first.result(10)
            second.result(10)
            assert dimensions == [3, 4] and ack_lost
            assert intermediate.calls == 0 and newest.calls == 1
            assert "pending_manifest" not in mfs._tasks.namespaces["n"]
            assert mfs.search("n", "needle", mode="vector").items
        finally:
            release.set()
            mfs.close()


def test_completion_storage_failure_retains_lease_and_reopens_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    calls: list[str] = []

    class Adapter(ContextAdapter):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext
        ) -> ProcessedDocument:
            calls.append(context.document_id.doc_id)
            if len(calls) == 1:
                entered.set()
                assert release.wait(10)
                context.checkpoint({"unit": 1})
            elif context.document_id.doc_id == "background.txt":
                assert context.resume_state == {"unit": 1}
            return ProcessedDocument("complete", SourceMap(1, ()))

    state = tmp_path / "state"
    adapter = Adapter()
    mfs = MFS.open(state)
    try:
        mfs.create_namespace("n", "internal", processors=[adapter])
        bg = mfs.upsert("n", "background.txt", b"source").id
        assert entered.wait(5)
        original = mfs._catalog.put_target

        def cannot_retire(ns: str, doc: str, job: dict[str, Any]) -> None:
            if doc == bg.doc_id and job["state"] == "pending" and job.get("checkpoint"):
                raise OSError("completion storage unavailable")
            original(ns, doc, job)

        monkeypatch.setattr(mfs._catalog, "put_target", cannot_retire)
        mfs.set_active_scopes([UnderPath("n", "active.txt")])
        mfs.upsert("n", "active.txt", b"source")
        release.set()
        with pytest.raises(StorageFailed, match="completion could not persist"):
            mfs.wait(bg, 10)
        assert calls == ["background.txt"] and mfs._tasks.executing
    finally:
        release.set()
        mfs.close()
    mfs = MFS.open(state)
    try:
        mfs.open_namespace("n", processors=[adapter])
        mfs.wait_ready(10)
        assert mfs.read(bg) is not None
    finally:
        mfs.close()


def test_aged_background_runs_then_resets_its_queue_age(tmp_path: Path) -> None:
    entered, release = threading.Event(), threading.Event()
    turns: list[str] = []

    class Adapter(ContextAdapter):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext
        ) -> ProcessedDocument:
            name = context.document_id.doc_id
            turns.append(name)
            if context.resume_state is None:
                if name == "active.txt":
                    entered.set()
                    assert release.wait(10)
                else:
                    # A selected target must not keep its old waiting advantage.
                    with mfs._condition:
                        assert context.document_id not in mfs._tasks.queued_at
                context.checkpoint({"unit": 1})
            return ProcessedDocument("complete", SourceMap(1, ()))

    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Adapter()])
        mfs.set_active_scopes([UnderPath("n", "active.txt")])
        mfs.upsert("n", "active.txt", b"active")
        assert entered.wait(5)
        bg = mfs.upsert("n", "background.txt", b"background").id
        with mfs._condition:
            revision, queued = mfs._tasks.queued_at[bg]
            mfs._tasks.queued_at[bg] = (revision, queued - 121)
            for cancellation in mfs._tasks.cancellations.values():
                cancellation._started_at -= 0.1
        release.set()
        mfs.wait_ready(10)
        assert turns == ["active.txt", "background.txt", "active.txt", "background.txt"]
    finally:
        release.set()
        mfs.close()
