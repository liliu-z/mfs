# pyright: reportPrivateUsage=false
from __future__ import annotations

import gc
import os
import sqlite3
import threading
import time
import weakref
from collections.abc import Generator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, ClassVar

import pytest

from mfs import (
    MFS,
    ChunkRange,
    DefaultChunker,
    DocumentId,
    ExecutionPolicy,
    GCPolicy,
    GrepBudget,
    NamespaceCompatibilityError,
    OperationFailed,
    PdfProcessor,
    ProcessedDocument,
    SourceMap,
    SourceUnavailable,
    StorageFailed,
    TextMatch,
    Utf8TextProcessor,
    WaitTimeout,
)
from mfs._namespace import NamespaceBinding


class Model:
    dimension = 2
    resources: ClassVar[dict[str, int]] = {}

    def __init__(self, space: str) -> None:
        self.embedding_space = space
        self.entered, self.release = threading.Event(), threading.Event()
        self.release.set()

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        self.entered.set()
        assert self.release.wait(15)
        return [[1.0, 0.0] if "old" in t else [0.8, 0.2] for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return [1.0, 0.0]


def test_grep_deadline_bounds_slow_chunker_and_retains_its_execution(tmp_path: Path) -> None:
    class Slow(DefaultChunker):
        resources: ClassVar[dict[str, int]] = {}

        def __init__(self) -> None:
            super().__init__()
            self.slow = False
            self.entered, self.release = threading.Event(), threading.Event()

        def chunk(self, text: str, source_map: SourceMap) -> tuple[ChunkRange, ...]:
            if self.slow:
                self.entered.set()
                assert self.release.wait(10)
            return super().chunk(text, source_map)

    chunker = Slow()
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], chunker=chunker)
        mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
        chunker.slow = True
        with ThreadPoolExecutor(1) as pool:
            try:
                future = pool.submit(
                    mfs.grep, "n", [TextMatch("needle")], select="chunk", timeout=0.2
                )
                assert chunker.entered.wait(5)
                with pytest.raises(WaitTimeout):
                    future.result(1)
                assert not chunker.release.is_set()
            finally:
                chunker.release.set()


@pytest.mark.parametrize("candidate", [False, True])
def test_rebind_cannot_overwrite_promoted_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, candidate: bool
) -> None:
    old, new = Model("old"), Model("new")
    entered, release = threading.Event(), threading.Event()
    verify = NamespaceBinding.verify
    target_thread: list[int] = []

    def delayed(self: NamespaceBinding, namespace: str, expected: dict[str, Any]) -> None:
        verify(self, namespace, expected)
        if target_thread and threading.get_ident() == target_thread[0]:
            entered.set()
            assert release.wait(15)

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=old)
        mfs.wait(mfs.upsert("n", "a.txt", b"old text"), 10)
        new.release.clear()
        change = mfs.configure_namespace("n", embedder=new)
        assert new.entered.wait(5)
        monkeypatch.setattr(NamespaceBinding, "verify", delayed)

        def rebind() -> None:
            target_thread.append(threading.get_ident())
            mfs.open_namespace(
                "n",
                processors=[Utf8TextProcessor()],
                embedder=new if candidate else old,
                configuration_revision=change.revision if candidate else None,
            )

        with ThreadPoolExecutor(1) as pool:
            try:
                future = pool.submit(rebind)
                assert entered.wait(5)
                new.release.set()
                mfs.wait(change, 10)
                assert mfs.namespace_configuration("n").active_revision == change.revision
            finally:
                new.release.set()
                release.set()
            with pytest.raises(NamespaceCompatibilityError):
                future.result(5)
        assert mfs.search("n", "text", mode="vector").items


def test_retained_old_snapshot_cannot_mask_current_hit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=Model("s"))
        mfs.wait(mfs.upsert("n", "a.txt", b"old needle"), 10)
        monkeypatch.setattr(mfs._index_cleanup, "maintain", lambda: None)
        mfs.upsert("n", "a.txt", b"new needle")
        result = mfs.search("n", "needle", mode="vector", consistency="strong", timeout=10)
        status = mfs.document_status(DocumentId("n", "a.txt"))
        assert status is not None and status.indexed_revision == status.revision
        assert len(mfs._runtime.index("n").scan()) == 2
        assert len(result.items) == 1 and result.items[0].value.text == "new needle"


def test_configuration_recovers_member_creation_failure_without_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], embedder=Model("old")
        )
        mfs.wait(mfs.upsert("n", "a.txt", b"old text"), 10)
        original = mfs._catalog.put_build
        armed = True

        def fail_once(namespace: str, doc_id: str, value: Any, **kwargs: Any) -> None:
            nonlocal armed
            if armed and value is not None:
                armed = False
                raise StorageFailed("injected candidate member write failure")
            original(namespace, doc_id, value, **kwargs)

        monkeypatch.setattr(mfs._catalog, "put_build", fail_once)
        new = Model("new")
        mfs.configure_namespace("n", embedder=new)
        replay = mfs.configure_namespace("n", embedder=new)
        mfs.wait(replay, 10)
        assert not armed
        assert mfs.namespace_configuration("n").active_revision == replay.revision
        assert mfs.search("n", "text", mode="vector").items


def test_configuration_adopts_member_after_lost_commit_acknowledgement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
        original = mfs._catalog.transaction
        armed = True

        @contextmanager
        def lose_ack() -> Generator[None]:
            nonlocal armed
            absent = mfs._catalog.get_build("n", "a.txt") is None
            with original():
                yield
            if armed and absent and mfs._catalog.get_build("n", "a.txt") is not None:
                armed = False
                raise StorageFailed("injected lost member commit acknowledgement")

        monkeypatch.setattr(mfs._catalog, "transaction", lose_ack)
        chunker = DefaultChunker()
        chunker.version = "2"
        change = mfs.configure_namespace("n", chunker=chunker)
        mfs.wait(change, 10)
        assert not armed
        assert mfs.namespace_configuration("n").active_revision == change.revision


def test_exhausted_configuration_exposes_error_and_same_request_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from mfs._index import ChunkIndex

    recreate = ChunkIndex.recreate
    attempts = 0

    def fail_five(self: ChunkIndex, *, dense_dimension: int | None) -> None:
        nonlocal attempts
        attempts += 1
        if attempts <= 5:
            raise StorageFailed("injected configuration initialization failure")
        recreate(self, dense_dimension=dense_dimension)

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
        monkeypatch.setattr(ChunkIndex, "recreate", fail_five)
        chunker = DefaultChunker()
        chunker.version = "2"
        change = mfs.configure_namespace("n", chunker=chunker)
        with pytest.raises(OperationFailed):
            mfs.wait(change, 15)
        config = mfs.namespace_configuration("n")
        assert config.pending_failures == 5
        assert config.pending_error and "initialization failure" in config.pending_error
        replay = mfs.configure_namespace("n", chunker=chunker)
        assert not replay.changed and replay.revision == change.revision
        mfs.wait(replay, 10)
        assert mfs.namespace_configuration("n").pending_error is None


def test_grep_remains_available_when_ranked_query_slots_are_stuck(tmp_path: Path) -> None:
    class StuckQuery(Model):
        concurrency = 2

        def embed_query(self, text: str) -> list[float]:
            with lock:
                entered.append(text)
                if len(entered) == 2:
                    both_entered.set()
            assert release.wait(10)
            return [1.0, 0.0]

    entered: list[str] = []
    lock = threading.Lock()
    both_entered, release = threading.Event(), threading.Event()
    with closing(MFS.open(tmp_path / "state", execution=ExecutionPolicy(queries=2))) as mfs:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], embedder=StuckQuery("s")
        )
        mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
        with ThreadPoolExecutor(2) as pool:
            pending = [
                pool.submit(mfs.search, "n", str(i), mode="vector", timeout=0.3) for i in range(2)
            ]
            try:
                assert both_entered.wait(5)
                for future in pending:
                    with pytest.raises(WaitTimeout):
                        future.result(2)
                assert mfs.grep("n", [TextMatch("needle")], timeout=1).items
            finally:
                release.set()


def test_aged_background_cannot_overtake_interactive_reprocess(tmp_path: Path) -> None:
    entered, release = threading.Event(), threading.Event()
    turns: list[str] = []

    class SerialText(Utf8TextProcessor):
        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            value = staged_path.read_text()
            turns.append(value)
            if value == "block":
                entered.set()
                assert release.wait(10)
            return super().process(staged_path, media_type)

    with closing(MFS.open(tmp_path / "state", execution=ExecutionPolicy(workers=1))) as mfs:
        try:
            mfs.create_namespace("n", "internal", processors=[SerialText()])
            mfs.upsert("n", "block.txt", b"block")
            assert entered.wait(5)
            background = mfs.upsert("n", "background.txt", b"background").id
            interactive = mfs.upsert("n", "interactive.txt", b"interactive").id
            mfs.reprocess(interactive)
            with mfs._condition:
                revision, queued = mfs._tasks.queued_at[background]
                mfs._tasks.queued_at[background] = (revision, queued - 3600)
            release.set()
            mfs.wait("n", 10)
            assert turns == ["block", "interactive", "background"]
        finally:
            release.set()


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO")
def test_sync_never_reopens_sniffed_source_under_state_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "source"
    root.mkdir()
    source = root / "document.unknown"
    source.write_bytes(b"%PDF-1.4\nfixture")
    replaced = threading.Event()
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n", "external", root, processors=[PdfProcessor()], processing_paused=True
        )
        mfs.create_namespace("healthy", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("healthy", "a.txt", b"needle"), 10)
        original = mfs._admit

        def swap(identity: DocumentId, staged: Any, **kwargs: Any) -> Any:
            if identity.namespace == "n":
                source.unlink()
                os.mkfifo(source)
                replaced.set()
            return original(identity, staged, **kwargs)

        monkeypatch.setattr(mfs, "_admit", swap)
        with ThreadPoolExecutor(2) as pool:
            scanning = pool.submit(mfs.sync, "n")
            try:
                assert replaced.wait(5)
                assert mfs.grep("healthy", [TextMatch("needle")], timeout=1).items
                pool.submit(mfs.status).result(1)
                scanning.result(1)
            finally:
                # Unblock the pre-fix FIFO read even when a regression assertion fails.
                try:
                    descriptor = os.open(source, os.O_WRONLY | os.O_NONBLOCK)
                except OSError:
                    pass
                else:
                    os.write(descriptor, b"%PDF-1.4\nfixture")
                    os.close(descriptor)


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO")
def test_internal_copy_rejects_fifo_replacement_without_waiting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.txt"
    source.write_text("needle")
    original = Path.lstat
    replaced = False

    def swap(path: Path) -> os.stat_result:
        nonlocal replaced
        observed = original(path)
        if path == source and not replaced:
            replaced = True
            source.unlink()
            os.mkfifo(source)
        return observed

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        monkeypatch.setattr(Path, "lstat", swap)
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(mfs.upsert, "n", "a.txt", source)
            try:
                with pytest.raises(SourceUnavailable):
                    pending.result(1)
            finally:
                try:
                    descriptor = os.open(source, os.O_WRONLY | os.O_NONBLOCK)
                except OSError:
                    pass
                else:
                    os.write(descriptor, b"needle")
                    os.close(descriptor)


@pytest.mark.parametrize("external", [False, True])
def test_processor_sniff_does_not_hold_state_lock(tmp_path: Path, external: bool) -> None:
    entered, release = threading.Event(), threading.Event()
    calls = 0

    class SlowSniff(Utf8TextProcessor):
        def sniff(self, head: bytes) -> str | None:
            nonlocal calls
            calls += 1
            entered.set()
            assert release.wait(10)
            return "text/plain"

    source = tmp_path / "source"
    source.mkdir()
    (source / "a.unknown").write_text("sniffed")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n",
            "external" if external else "internal",
            source if external else None,
            processors=[SlowSniff()],
            processing_paused=True,
        )
        mfs.create_namespace("healthy", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("healthy", "a.txt", b"needle"), 10)
        with ThreadPoolExecutor(2) as pool:
            pending = (
                pool.submit(mfs.sync, "n")
                if external
                else pool.submit(mfs.upsert, "n", "a.unknown", b"sniffed")
            )
            try:
                assert entered.wait(5)
                pool.submit(mfs.status).result(1)
                assert mfs.grep("healthy", [TextMatch("needle")], timeout=1).items
            finally:
                release.set()
            pending.result(5)
            assert calls == 1


def test_persistent_promotion_failure_reaches_retry_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempts = 0
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
        original = mfs._catalog.put_namespace

        def fail_promotion(namespace: str, record: dict[str, Any]) -> None:
            nonlocal attempts
            if (
                "building" not in record
                and record["manifest"]["index"]["chunker"]["version"] == "2"
            ):
                attempts += 1
                raise StorageFailed("injected promotion failure")
            original(namespace, record)

        monkeypatch.setattr(mfs._catalog, "put_namespace", fail_promotion)
        chunker = DefaultChunker()
        chunker.version = "2"
        change = mfs.configure_namespace("n", chunker=chunker)
        with pytest.raises(OperationFailed):
            mfs.wait(change, 15)
        assert attempts == 5
        assert mfs.namespace_configuration("n").pending_failures == 5


def test_transient_text_reconstruction_uses_processor_admission(tmp_path: Path) -> None:
    entered, concurrent, release = threading.Event(), threading.Event(), threading.Event()
    lock = threading.Lock()
    active = maximum = 0

    class Transient(Utf8TextProcessor):
        concurrency = 1
        workload = "heavy"

        def __init__(self) -> None:
            super().__init__()
            self.block = False

        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
                if active > 1:
                    concurrent.set()
            try:
                if self.block:
                    entered.set()
                    assert release.wait(10)
                original = super().process(staged_path, media_type)
                return ProcessedDocument(original.text, original.source_map, grep_path=staged_path)
            finally:
                with lock:
                    active -= 1

    processor = Transient()
    with closing(MFS.open(tmp_path / "state")) as mfs:
        try:
            mfs.create_namespace("n", "internal", processors=[processor])
            for name in ("a.txt", "b.txt"):
                mfs.wait(mfs.upsert("n", name, name.encode()), 10)
            processor.block = True
            chunker = DefaultChunker()
            chunker.version = "2"
            change = mfs.configure_namespace("n", chunker=chunker)
            assert entered.wait(5)
            assert not concurrent.wait(0.3)
            with mfs._condition:
                assert mfs._runtime._adapter_used.get(id(processor)) == 1
                assert all(j["stage"] == "process" for j in mfs._tasks.execution_records.values())
            release.set()
            mfs.wait(change, 10)
            assert maximum == 1
        finally:
            release.set()


def test_dropped_namespace_releases_configured_model_objects(tmp_path: Path) -> None:
    references: list[weakref.ReferenceType[Model]] = []
    with closing(MFS.open(tmp_path / "state")) as mfs:
        for generation in range(3):
            mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
            model = Model(str(generation))
            references.append(weakref.ref(model))
            mfs.wait(mfs.configure_namespace("n", embedder=model, indexing="hybrid"), 10)
            mfs.wait(mfs.drop_namespace("n"), 10)
            del model
        gc.collect()
        assert not any(reference() is not None for reference in references)


def test_grep_is_partial_when_one_external_file_disappears(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    (root / "a.txt").write_text("needle one")
    (root / "b.txt").write_text("needle two")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "external", root, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("n"), 10)
        (root / "a.txt").unlink()
        result = mfs.grep("n", [TextMatch("needle")])
        assert [item.value for item in result.items] == [DocumentId("n", "b.txt")]
        assert result.truncated
        assert result.failures[0].id == DocumentId("n", "a.txt")
        assert result.failures[0].error.code == "SourceUnavailable"


def test_plain_text_grep_obeys_deadline_and_maps_dense_matches(tmp_path: Path) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        mfs.wait(mfs.upsert("n", "a.txt", b"needle\n" * 20000), 20)
        started = time.monotonic()
        with pytest.raises(WaitTimeout):
            mfs.grep("n", [TextMatch("needle")], timeout=0.01)
        assert time.monotonic() - started < 0.5
        result = mfs.grep(
            "n", [TextMatch("needle")], timeout=3, budget=GrepBudget(max_matches=5000)
        )
        assert len(result.items[0].matches) == 5000
        assert result.items[0].matches[-1].source_location.sources == (
            {"kind": "lines", "start": 5000, "end": 5000},
        )


def test_gc_treats_writer_contention_as_busy(tmp_path: Path) -> None:
    with closing(
        MFS.open(tmp_path / "state", gc_policy=GCPolicy(enabled=False, idle_seconds=0))
    ) as mfs:
        with closing(sqlite3.connect(tmp_path / "state" / "catalog.sqlite")) as writer:
            writer.execute("BEGIN IMMEDIATE")
            try:
                with mfs._condition:
                    report = mfs.collect_garbage()
            finally:
                writer.rollback()
        assert report.busy and report.error is None


def test_expired_query_does_not_enter_backend_after_lock_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
        client = mfs._runtime.index("n").client
        raw = client._client
        original = raw.search
        backend_calls: list[float] = []

        def raw_search(*args: Any, **kwargs: Any) -> Any:
            backend_calls.append(time.monotonic())
            return original(*args, **kwargs)

        def block_other_call() -> None:
            with client._lock:
                entered.set()
                assert release.wait(10)

        monkeypatch.setattr(raw, "search", raw_search)
        with ThreadPoolExecutor(1) as pool:
            held = pool.submit(block_other_call)
            try:
                assert entered.wait(5)
                with pytest.raises(WaitTimeout):
                    mfs.search("n", "needle", mode="bm25", timeout=0.1)
                assert not backend_calls
            finally:
                release.set()
            held.result(5)
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: not mfs._tasks.queries, 5)
        assert not backend_calls


@pytest.mark.parametrize("replacement", ["file_link", "parent_link", "fifo"])
def test_external_text_reads_reject_replaced_paths(tmp_path: Path, replacement: str) -> None:
    import os

    if replacement == "fifo" and os.name == "nt":
        pytest.skip("POSIX FIFO")
    root = tmp_path / "source"
    root.mkdir()
    folder = root / "folder"
    folder.mkdir()
    source = folder / "a.txt"
    source.write_text("public original")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "a.txt").write_text("outside marker")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "external", root, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("n"), 10)
        source.unlink()
        if replacement == "file_link":
            source.symlink_to(outside / "a.txt")
        elif replacement == "parent_link":
            folder.rmdir()
            folder.symlink_to(outside, target_is_directory=True)
        else:
            os.mkfifo(source)
        # grep must not follow the replacement or block on a FIFO open.
        result = mfs.grep("n", [TextMatch("outside")], timeout=1)
        assert not result.items and result.truncated
        assert result.failures[0].error.code == "SourceUnavailable"
        with pytest.raises(SourceUnavailable):
            mfs.read(DocumentId("n", "folder/a.txt"))
