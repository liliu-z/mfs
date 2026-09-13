# pyright: reportPrivateUsage=false
from __future__ import annotations

import threading
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

import pytest

from mfs import (
    MFS,
    CapabilityUnavailable,
    DocumentId,
    GrepResult,
    InvalidQuery,
    OperationFailed,
    ProcessedDocument,
    ProcessingContext,
    StorageFailed,
    TaskError,
    UnderPath,
    Utf8TextProcessor,
    WaitTimeout,
)


def test_scope_lease_waits_for_retirement_and_preserves_user_intent(tmp_path: Path) -> None:
    entered, retiring, release = (threading.Event() for _ in range(3))
    calls: list[str] = []

    class Processor(Utf8TextProcessor):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext | None = None
        ) -> ProcessedDocument:
            assert context is not None
            calls.append(context.document_id.doc_id)
            if len(calls) == 1:
                entered.set()
                try:
                    assert context.cancellation.wait(10)
                    context.cancellation.check()
                finally:
                    retiring.set()
                    assert release.wait(10)
            return super().process(staged_path, media_type)

    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Processor()])
        first = mfs.upsert("n", "a.txt", b"before mutation")
        assert entered.wait(5)
        with ThreadPoolExecutor() as pool:
            waiting = pool.submit(mfs.quiesce, [UnderPath("n", "a.txt")], 10)
            assert retiring.wait(5)
            assert not waiting.done()
            release.set()
            with waiting.result(10):
                with mfs.quiesce([UnderPath("n", "a.txt")], 1):
                    mfs.upsert("n", "a.txt", b"after mutation")
                    mfs.wait(mfs.upsert("n", "b.txt", b"unrelated"), 10)
                assert calls.count("a.txt") == 1  # Nested release leaves the outer gate intact.
                with pytest.raises(WaitTimeout):
                    mfs.wait(first, 0)
            mfs.wait(first, 10)
        document = mfs.read(first.id)
        assert document is not None and document.text == "after mutation"
        assert calls.count("a.txt") == 2
    finally:
        release.set()
        mfs.close()


def test_scope_lease_timeout_releases_only_its_gate(tmp_path: Path) -> None:
    entered, release = threading.Event(), threading.Event()

    class Processor(Utf8TextProcessor):
        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            entered.set()
            assert release.wait(10)
            return super().process(staged_path, media_type)

    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Processor()])
        report = mfs.upsert("n", "a.txt", b"retry after timed out quiesce")
        assert entered.wait(5)
        with pytest.raises(WaitTimeout):
            mfs.quiesce([UnderPath("n")], 0)
        release.set()
        mfs.wait(report, 10)
        assert mfs.read(report.id) is not None
    finally:
        release.set()
        mfs.close()


def test_quiescence_waits_for_source_reads_and_blocks_new_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        report = mfs.upsert("n", "a.txt", b"readable")
        mfs.wait(report, 10)
        original = mfs._artifacts.read_text

        def blocked(record: dict[str, object], *, grep: bool = False) -> str:
            entered.set()
            assert release.wait(10)
            return original(record, grep=grep)

        monkeypatch.setattr(mfs._artifacts, "read_text", blocked)
        with ThreadPoolExecutor() as pool:
            read = pool.submit(mfs.read, report.id)
            assert entered.wait(5)
            waiting = pool.submit(mfs.quiesce, [UnderPath("n")], 10)
            with mfs._condition:
                assert mfs._condition.wait_for(lambda: bool(mfs._tasks.quiescence), 5)
            assert not waiting.done()
            release.set()
            assert read.result(10) is not None
            with waiting.result(10):
                with pytest.raises(CapabilityUnavailable, match="quiesced"):
                    mfs.read(report.id)
                assert mfs.search("n", "readable", mode="bm25").items
        assert mfs.read(report.id) is not None
    finally:
        release.set()
        mfs.close()


def test_migration_processing_gate_and_imported_states_survive_reopen(tmp_path: Path) -> None:
    state = tmp_path / "state"
    with closing(MFS.open(state)) as mfs:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], processing_paused=True
        )
        failed = mfs.upsert("n", "failed.txt", b"previous failure")
        cancelled = mfs.upsert("n", "cancelled.txt", b"previous cancellation")
        ready = mfs.upsert("n", "ready.txt", b"ready for migration")
        assert failed.revision is not None and cancelled.revision is not None
        mfs.restore_document_state(
            failed.id,
            expected_revision=failed.revision,
            state="failed",
            error=TaskError("OldFailure", "previous extraction failed", False),
        )
        mfs.restore_document_state(
            cancelled.id, expected_revision=cancelled.revision, state="cancelled"
        )
        assert not mfs.grep("n").items
    with closing(MFS.open(state)) as mfs:
        mfs.open_namespace("n", processors=[Utf8TextProcessor()])
        assert mfs.namespace_configuration("n").processing_paused
        with pytest.raises(WaitTimeout):
            mfs.wait(ready, 0)
        mfs.restore_document_state(
            cancelled.id, expected_revision=cancelled.revision, state="cancelled"
        )
        mfs.retry(cancelled.id)
        # A delayed/replayed migration record cannot undo explicit user retry.
        mfs.restore_document_state(
            cancelled.id, expected_revision=cancelled.revision, state="cancelled"
        )
        mfs.configure_processing("n", paused=False)
        mfs.wait(ready, 10)
        mfs.wait(cancelled, 10)
        with pytest.raises(OperationFailed) as caught:
            mfs.wait(failed, 0)
        assert caught.value.error_code == "OldFailure"
        mfs.retry(failed.id)
        mfs.wait(failed, 10)
        assert len(mfs.grep("n").items) == 3


def test_import_rejects_stale_revision_and_requires_processing_gate(tmp_path: Path) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], processing_paused=True
        )
        old = mfs.upsert("n", "a.txt", b"old")
        new = mfs.upsert("n", "a.txt", b"new")
        assert old.revision is not None and new.revision is not None
        with pytest.raises(InvalidQuery, match="revision"):
            mfs.restore_document_state(old.id, expected_revision=old.revision, state="cancelled")
        mfs.configure_processing("n", paused=False)
        mfs.wait(new, 10)
        with pytest.raises(InvalidQuery, match="processing_paused"):
            mfs.restore_document_state(new.id, expected_revision=new.revision, state="cancelled")


def test_scope_lease_does_not_bind_a_recreated_namespace(tmp_path: Path) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        lease = mfs.quiesce([UnderPath("n")])
        try:
            mfs.drop_namespace("n")
            mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
            report = mfs.upsert("n", "new.txt", b"new incarnation")
            # The old drop remains gated, but the new incarnation can prepare/index.
            with mfs._condition:
                assert mfs._condition.wait_for(
                    lambda: mfs._tasks.targets[report.id]["state"] == "succeeded", 10
                )
        finally:
            lease.close()
        mfs.wait("n", 10)
        assert mfs.read(DocumentId("n", "new.txt")) is not None


@pytest.mark.parametrize("operation", ["read", "grep"])
def test_late_source_read_cannot_cross_namespace_retirement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    entered, release = threading.Event(), threading.Event()
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("old source")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "external", source, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("n"), 10)
        original = mfs._view.source_read

        @contextmanager
        def delayed(*args: Any, **kwargs: Any) -> Generator[Any]:
            entered.set()
            assert release.wait(10)
            with original(*args, **kwargs) as admitted:
                yield admitted

        monkeypatch.setattr(mfs._view, "source_read", delayed)
        with ThreadPoolExecutor() as pool:
            reading = (
                pool.submit(mfs.read, DocumentId("n", "a.txt"))
                if operation == "read"
                else pool.submit(mfs.grep, "n", select="doc")
            )
            try:
                assert entered.wait(5)
                with mfs.quiesce([UnderPath("n")], 1):
                    mfs.drop_namespace("n")
                    mfs.create_namespace("n", "external", source, processors=[Utf8TextProcessor()])
                    # No source access is allowed for a stale record, even when a
                    # new namespace incarnation no longer matches the old gate.
                    (source / "a.txt").unlink()
                    release.set()
                    result = reading.result(10)
                    if operation == "read":
                        assert result is None
                    else:
                        assert isinstance(result, GrepResult) and not result.items
            finally:
                release.set()


@pytest.mark.parametrize("lost_ack", [False, True])
def test_processing_pause_retires_running_attempt_without_user_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lost_ack: bool
) -> None:
    entered = threading.Event()

    class Processor(Utf8TextProcessor):
        calls = 0

        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext | None = None
        ) -> ProcessedDocument:
            self.calls += 1
            if self.calls == 1:
                assert context is not None
                context.checkpoint({"prepared_unit": 1})
                entered.set()
                assert context.cancellation.wait(10)
                context.cancellation.check()
            else:
                assert context is not None and context.resume_state == {"prepared_unit": 1}
            return super().process(staged_path, media_type)

    processor = Processor()
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[processor])
        report = mfs.upsert("n", "a.txt", b"resume paused work")
        assert entered.wait(5)
        original = mfs._catalog.transaction
        owner = threading.get_ident()

        @contextmanager
        def committed_error() -> Generator[None]:
            with original():
                yield
            if threading.get_ident() == owner:
                raise StorageFailed("pause committed without acknowledgement")

        with monkeypatch.context() as patch:
            if lost_ack:
                patch.setattr(mfs._catalog, "transaction", committed_error)
                with pytest.raises(StorageFailed):
                    mfs.configure_processing("n", paused=True)
            else:
                mfs.configure_processing("n", paused=True)
        with mfs.quiesce([UnderPath("n")], 5):
            assert mfs.namespace_configuration("n").processing_paused
        status = mfs.document_status(report.id)
        assert status is not None and status.state == "pending" and not status.executing
        with pytest.raises(WaitTimeout):
            mfs.wait(report, 0)
        mfs.configure_processing("n", paused=False)
        mfs.wait(report, 10)
        assert processor.calls == 2
