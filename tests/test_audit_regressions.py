# pyright: reportPrivateUsage=false
from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Generator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

import pytest
from test_namespace_contract import Model

from mfs import (
    MFS,
    Cancellation,
    DocumentId,
    GCPolicy,
    IgnoreRule,
    OperationFailed,
    ProcessedDocument,
    ProcessingContext,
    SourceMap,
    StorageFailed,
    Utf8TextProcessor,
    WaitTimeout,
    _sync,
)


@pytest.mark.parametrize("action", ["remove", "drop", "exclude", "cancel"])
def test_committed_invalidation_with_lost_ack_retires_old_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    entered, release = threading.Event(), threading.Event()

    class Processor(Utf8TextProcessor):
        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            entered.set()
            assert release.wait(10)
            return super().process(staged_path, media_type)

    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Processor()])
        report = mfs.upsert("n", "a.txt", b"must not resurrect")
        assert entered.wait(5)
        original = mfs._catalog.transaction
        owner = threading.get_ident()
        armed = True

        @contextmanager
        def lost_ack() -> Generator[None]:
            nonlocal armed
            with original():
                yield
            if armed and threading.get_ident() == owner:
                armed = False
                raise StorageFailed("injected lost acknowledgement")

        with monkeypatch.context() as patch:
            patch.setattr(mfs._catalog, "transaction", lost_ack)
            with pytest.raises(StorageFailed):
                if action == "remove":
                    mfs.remove("n", "a.txt")
                elif action == "drop":
                    mfs.drop_namespace("n")
                elif action == "exclude":
                    mfs.update_rules(
                        "n",
                        expected_revision=mfs.rules("n").revision,
                        add=[IgnoreRule("hide", "a.txt")],
                    )
                else:
                    mfs.cancel(report.id)
        release.set()
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: not mfs._tasks.executing, 10)
        if action == "drop":
            assert not mfs.list_namespaces()
            mfs.wait("n", 10)
        else:
            if action == "cancel":
                with pytest.raises(OperationFailed) as caught:
                    mfs.wait(report, 10)
                assert caught.value.state == "cancelled"
            else:
                mfs.wait(report, 10)
            assert mfs.read(report.id) is None
            assert not mfs.search("n", "resurrect", mode="bm25").items
    finally:
        release.set()
        mfs.close()


def test_external_processing_cannot_poison_content_cache(tmp_path: Path) -> None:
    entered, release = threading.Event(), threading.Event()

    class Processor(Utf8TextProcessor):
        calls = 0

        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            self.calls += 1
            if self.calls == 1:
                entered.set()
                assert release.wait(10)
            return ProcessedDocument("extracted " + staged_path.read_text(), SourceMap(1, ()))

    source = tmp_path / "source"
    source.mkdir()
    path = source / "a.txt"
    path.write_text("alpha")
    processor = Processor()
    mfs = MFS.open(tmp_path / "state", gc_policy=GCPolicy(enabled=False))
    try:
        mfs.create_namespace("n", "external", source, processors=[processor])
        report = mfs.sync("n", verify="content")
        assert entered.wait(5)
        path.write_text("bravo")
        release.set()
        with pytest.raises(OperationFailed) as caught:
            mfs.wait(report, 10)
        assert caught.value.error_code == "SourceChanged"
        assert mfs.read(DocumentId("n", "a.txt")) is None
        mfs.wait(mfs.sync("n", verify="content"), 10)
        path.write_text("alpha")
        mfs.wait(mfs.sync("n", verify="content"), 10)
        document = mfs.read(DocumentId("n", "a.txt"))
        assert document is not None and document.text == "extracted alpha"
        assert not mfs.search("n", "bravo", mode="bm25").items
    finally:
        release.set()
        mfs.close()


def test_unreadable_commit_outcome_stops_publication_until_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()

    class Processor(Utf8TextProcessor):
        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            entered.set()
            assert release.wait(10)
            return super().process(staged_path, media_type)

    state = tmp_path / "state"
    mfs = MFS.open(state)
    try:
        mfs.create_namespace("n", "internal", processors=[Processor()])
        report = mfs.upsert("n", "a.txt", b"old execution cannot publish")
        assert entered.wait(5)
        original = mfs._catalog.transaction

        @contextmanager
        def committed_error() -> Generator[None]:
            with original():
                yield
            raise StorageFailed("lost acknowledgement")

        def unreadable() -> Any:
            raise StorageFailed("cannot read back committed targets")

        with monkeypatch.context() as patch:
            patch.setattr(mfs._catalog, "transaction", committed_error)
            patch.setattr(mfs._catalog, "list_targets", unreadable)
            with pytest.raises(StorageFailed, match="lost acknowledgement"):
                mfs.remove("n", "a.txt")
        release.set()
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: not mfs._tasks.executing, 10)
        assert mfs.status().index_state == "dirty"
        with pytest.raises(StorageFailed, match="cannot reconcile"):
            mfs.search("n", "old", mode="bm25")
        with pytest.raises(StorageFailed, match="cannot reconcile"):
            mfs.read(report.id)
        durable = mfs._catalog.get_target("n", "a.txt")
        assert durable is not None and durable["kind"] == "delete"
    finally:
        release.set()
        mfs.close()
    with closing(MFS.open(state)) as reopened:
        reopened.open_namespace("n", processors=[Utf8TextProcessor()])
        reopened.wait("n", 10)
        assert reopened.read(DocumentId("n", "a.txt")) is None
        assert not reopened.search("n", "old", mode="bm25").items


@pytest.mark.parametrize("checkpoint", [False, True])
def test_unobserved_source_change_and_restore_cannot_publish(
    tmp_path: Path, checkpoint: bool
) -> None:
    class Processor(Utf8TextProcessor):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext | None = None
        ) -> ProcessedDocument:
            assert context is not None
            metadata = staged_path.stat()
            staged_path.write_text("bravo")
            text = staged_path.read_text()
            staged_path.write_text("alpha")
            os.utime(staged_path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
            if checkpoint:
                context.checkpoint({"incorrect": text})
            return ProcessedDocument(text, SourceMap(1, ()))

    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("alpha")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "external", source, processors=[Processor()])
        report = mfs.sync("n")
        with pytest.raises(OperationFailed) as caught:
            mfs.wait(report, 10)
        assert caught.value.error_code == "SourceChanged"
        assert not mfs._tasks.targets[DocumentId("n", "a.txt")].get("checkpoint")
        assert not mfs.grep("n").items


def test_borrowed_text_change_between_preparation_and_indexing_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    path = source / "a.txt"
    path.write_text("alpha")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "external", source, processors=[Utf8TextProcessor()])
        mfs.configure_index("n", paused=True)
        report = mfs.sync("n")
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs._tasks.targets[DocumentId("n", "a.txt")]["stage"] == "chunk", 10
            )
        path.write_text("bravo")
        mfs.configure_index("n", paused=False)
        with pytest.raises(OperationFailed) as caught:
            mfs.wait(report, 10)
        assert caught.value.error_code == "SourceChanged"
        assert not mfs.search("n", "bravo", mode="bm25").items


def test_rebuild_lost_ack_adopts_the_committed_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        report = mfs.upsert("n", "a.txt", b"rebuild with the new model")
        mfs.wait(report, 10)
        original = mfs._catalog.transaction
        owner = threading.get_ident()
        armed = True

        @contextmanager
        def lost_ack() -> Generator[None]:
            nonlocal armed
            with original():
                yield
            if armed and threading.get_ident() == owner:
                armed = False
                raise StorageFailed("committed rebuild lost acknowledgement")

        with monkeypatch.context() as patch:
            patch.setattr(mfs._catalog, "transaction", lost_ack)
            with pytest.raises(StorageFailed):
                mfs.reindex("n", embedder=Model(3, "new"), indexing="hybrid")
        mfs.wait(report, 10)
        assert mfs.search("n", "model", mode="vector").items


def test_cancelled_publication_restarts_index_stages_after_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        index = mfs._runtime.index("n")
        original = index.publish

        def gated(*args: Any, **kwargs: Any) -> Any:
            entered.set()
            assert release.wait(10)
            return original(*args, **kwargs)

        monkeypatch.setattr(index, "publish", gated)
        report = mfs.upsert("n", "a.txt", b"recover cancelled publication")
        assert entered.wait(5)
        mfs.cancel(report.id)
        # Candidate membership expands asynchronously. With the scheduler excluded,
        # a zero budget sees pending intent before cancellation has been copied.
        with mfs._condition, pytest.raises(WaitTimeout):
            mfs.reindex("n", timeout=0)
        with pytest.raises(OperationFailed, match="cancelled"):
            mfs.wait(report, 5)
        release.set()
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: not mfs._tasks.executing, 10)
        status = mfs.document_status(report.id)
        assert status is not None and status.state == "cancelled"
        assert not mfs.search("n", "recover", mode="bm25", consistency="eventual").items
        mfs.retry(report.id)
        mfs.wait(report, 10)
        assert mfs.search("n", "recover", mode="bm25").items
    finally:
        release.set()
        mfs.close()


def test_strong_search_reports_terminal_failure(tmp_path: Path) -> None:
    class Broken(Utf8TextProcessor):
        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            raise ValueError("permanent extraction failure")

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Broken()])
        report = mfs.upsert("n", "a.txt", b"failed")
        with pytest.raises(OperationFailed):
            mfs.wait(report, 10)
        with pytest.raises(OperationFailed) as caught:
            mfs.search("n", "failed", mode="bm25", consistency="strong", timeout=1)
        assert caught.value.revision == report.revision
        assert caught.value.error_code == "ValueError"


def test_managed_grep_text_survives_gc_and_reopen(tmp_path: Path) -> None:
    class Views(Utf8TextProcessor):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext | None = None
        ) -> ProcessedDocument:
            assert context is not None
            grep = context.work_dir / "grep.txt"
            grep.write_text("readable raw view")
            return ProcessedDocument("index view", SourceMap(1, ()), grep_path=grep)

    state = tmp_path / "state"
    with closing(
        MFS.open(
            state,
            gc_policy=GCPolicy(
                enabled=False,
                idle_seconds=0,
                grace_seconds=0,
            ),
        )
    ) as mfs:
        mfs.create_namespace("n", "internal", processors=[Views()], indexing="off")
        report = mfs.upsert("n", "a.txt", b"original")
        mfs.wait(report, 10)
        for _ in range(5):
            mfs.collect_garbage()
        document = mfs.read(report.id)
        assert document is not None and document.text == "readable raw view"
        assert document.source_map == mfs.grep("n", select="doc").items[0].value.source_map
    with closing(MFS.open(state)) as reopened:
        document = reopened.read(report.id)
        assert document is not None and document.text == "readable raw view"
        assert document.source_map.spans[0].source == {"kind": "lines", "start": 1, "end": 1}


def test_partial_scan_removes_missing_files_only_in_observed_regions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source"
    (source / "denied").mkdir(parents=True)
    (source / "denied/keep.txt").write_text("protected")
    (source / "gone.txt").write_text("deletedneedle")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "external", source, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("n"), 10)
        (source / "gone.txt").unlink()
        original = _sync.files.open

        def denied(path: Any, *args: Any, **kwargs: Any) -> int:
            if path == "denied":
                raise PermissionError("injected unreadable directory")
            return original(path, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(_sync.files, "open", denied)
            report = mfs.sync("n")
        assert not report.complete
        assert report.removed == (DocumentId("n", "gone.txt"),)
        assert not mfs.search("n", "deletedneedle", mode="bm25").items
        document = mfs.read(DocumentId("n", "denied/keep.txt"))
        assert document is not None and document.text == "protected"


@pytest.mark.skipif(os.name != "posix", reason="POSIX parent-death process supervision")
def test_managed_process_retires_when_mfs_host_is_killed(tmp_path: Path) -> None:
    script = r"""
import sys, time
from pathlib import Path
from mfs import MFS, Utf8TextProcessor, ProcessedDocument
state, marker = map(Path, sys.argv[1:])
class Processor(Utf8TextProcessor):
    def process(self, path, media_type, context):
        context.run_process([sys.executable, '-c',
            'import os,sys,time; from pathlib import Path; '
            'Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(60)', str(marker)])
        return super().process(path, media_type)
mfs=MFS.open(state)
mfs.create_namespace('n','internal',processors=[Processor()])
mfs.wait(mfs.upsert('n','a.txt',b'recover after parent death'))
"""
    state, marker = tmp_path / "state", tmp_path / "pid"
    host = subprocess.Popen(
        [sys.executable, "-c", script, str(state), str(marker)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    pid = None
    try:
        deadline = time.monotonic() + 15
        while not marker.exists() and time.monotonic() < deadline:
            assert host.poll() is None
            time.sleep(0.025)
        assert marker.exists()
        pid = int(marker.read_text())
        host.kill()
        host.wait(5)

        def running() -> bool:
            result = subprocess.run(
                ["/bin/ps", "-p", str(pid), "-o", "stat="],
                capture_output=True,
                text=True,
                check=False,
            ).stdout.strip()
            return bool(result) and not result.startswith("Z")

        deadline = time.monotonic() + 5
        while running() and time.monotonic() < deadline:
            time.sleep(0.025)
        assert not running(), "managed child survived the MFS host"
        from mfs import InstanceLocked

        while True:
            try:
                reopened = MFS.open(state)
                break
            except InstanceLocked:
                assert time.monotonic() < deadline
                time.sleep(0.025)
        with closing(reopened):
            reopened.open_namespace("n", processors=[Utf8TextProcessor()])
            reopened.wait("n", 10)
            assert reopened.search("n", "recover", mode="bm25").items
    finally:
        if host.poll() is None:
            host.kill()
        host.wait()
        if pid is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pid, signal.SIGKILL)


@pytest.mark.skipif(os.name != "posix", reason="POSIX frozen supervisor dispatch")
@pytest.mark.parametrize("frozen", [False, True])
def test_frozen_supervisor_dispatch_runs_the_command_without_booting_the_app(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, frozen: bool
) -> None:
    interpreter = sys.executable
    executable = tmp_path / "sidecar"
    executable.write_text(
        f"#!{interpreter}\n"
        "from mfs import run_process_supervisor\n"
        "run_process_supervisor()\n"
        "raise AssertionError('supervisor unexpectedly booted the application')\n"
    )
    executable.chmod(0o700)
    context = ProcessingContext(
        DocumentId("n", "a.txt"),
        "revision",
        "hash",
        tmp_path,
        Cancellation(),
        None,
        {},
        lambda state, files: False,
        lambda completed, total, unit: None,
    )
    if frozen:
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        monkeypatch.setattr(sys, "executable", str(executable))
    result = context.run_process([interpreter, "-c", "print('native result')"], timeout=5)
    assert result.returncode == 0 and result.stdout == b"native result\n"
