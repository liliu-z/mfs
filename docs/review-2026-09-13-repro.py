"""HISTORICAL, pre-fix audit: passing means the reviewed defect was observed.

These are diagnostic artifacts, not assertions of desired behavior. They use only
temporary stores and deterministic local models. Run from the repository root:

    .venv/bin/python -m pytest -q -s --tb=short --show-capture=no docs/review-2026-09-13-repro.py

These defects now have correctness regressions in tests/test_review_boundaries.py.
Run that file against current code. This original diagnostic is retained as
historical evidence; its failure against the fixed implementation is expected.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from typing import ClassVar

import pytest

from mfs import (
    MFS,
    DefaultChunker,
    DocumentId,
    NamespaceCompatibilityError,
    StorageFailed,
    TextMatch,
    Utf8TextProcessor,
    WaitTimeout,
)
from mfs._namespace import NamespaceBinding


class Model:
    dimension = 2
    resources: ClassVar[dict[str, int]] = {}

    def __init__(self, space):
        self.embedding_space = space
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()

    def embed_documents(self, texts):
        self.entered.set()
        assert self.release.wait(15)
        return [[1.0, 0.0] if "old" in t else [0.8, 0.2] for t in texts]

    def embed_query(self, text):
        return [1.0, 0.0]


def test_grep_timeout_does_not_bound_chunk_execution(tmp_path):
    class Slow(DefaultChunker):
        resources: ClassVar[dict[str, int]] = {}

        def __init__(self):
            super().__init__()
            self.slow = False
            self.entered = threading.Event()
            self.release = threading.Event()

        def chunk(self, text, source_map):
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
                start = time.monotonic()
                future = pool.submit(
                    mfs.grep, "n", [TextMatch("needle")], select="chunk", timeout=0.05
                )
                assert chunker.entered.wait(5)
                time.sleep(0.25)
                assert not future.done()
                print(
                    "OBSERVED grep still blocked beyond timeout:",
                    round(time.monotonic() - start, 3),
                )
            finally:
                chunker.release.set()
            assert future.result(5).items


def test_rebind_races_configuration_promotion(tmp_path, monkeypatch):
    old, new = Model("old"), Model("new")
    entered, release = threading.Event(), threading.Event()
    verify = NamespaceBinding.verify
    target_thread = []

    def delayed(self, namespace, expected):
        result = verify(self, namespace, expected)
        if target_thread and threading.get_ident() == target_thread[0]:
            entered.set()
            assert release.wait(15)
        return result

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=old)
        mfs.wait(mfs.upsert("n", "a.txt", b"old text"), 10)
        new.release.clear()
        change = mfs.configure_namespace("n", embedder=new)
        assert new.entered.wait(5)
        monkeypatch.setattr(NamespaceBinding, "verify", delayed)

        def rebind():
            target_thread.append(threading.get_ident())
            mfs.open_namespace("n", processors=[Utf8TextProcessor()], embedder=old)

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
            future.result(5)
        with pytest.raises(NamespaceCompatibilityError) as caught:
            mfs.search("n", "text", mode="vector")
        print("OBSERVED old binding overwrote promoted model:", str(caught.value))


def test_retained_old_snapshot_masks_current_hit(tmp_path, monkeypatch):
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], embedder=Model("same")
        )
        mfs.wait(mfs.upsert("n", "a.txt", b"old needle"), 10)
        monkeypatch.setattr(mfs._index_cleanup, "maintain", lambda: None)
        mfs.upsert("n", "a.txt", b"new needle")
        result = mfs.search("n", "needle", mode="vector", consistency="strong", timeout=10)
        status = mfs.document_status(DocumentId("n", "a.txt"))
        assert status.indexed_revision == status.revision
        rows = mfs._runtime.index("n").scan()
        assert len(rows) == 2
        assert not result.items
        print("OBSERVED current indexed document missing:", len(rows), "physical rows;", result)


def test_configuration_acceptance_can_leave_missing_build_targets(tmp_path, monkeypatch):
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], embedder=Model("old")
        )
        mfs.wait(mfs.upsert("n", "a.txt", b"old text"), 10)
        original = mfs._catalog.put_build
        owner = threading.get_ident()
        armed = True

        def fail_once(namespace, doc_id, value, **kwargs):
            nonlocal armed
            if armed and threading.get_ident() == owner and value is not None:
                armed = False
                raise StorageFailed("injected candidate target write failure")
            return original(namespace, doc_id, value, **kwargs)

        new = Model("new")
        with monkeypatch.context() as patch:
            patch.setattr(mfs._catalog, "put_build", fail_once)
            with pytest.raises(StorageFailed):
                mfs.configure_namespace("n", embedder=new)
        replay = mfs.configure_namespace("n", embedder=new)
        assert not replay.changed
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs._tasks.namespaces["n"]["building"]["initialized"], 10
            )
        with pytest.raises(WaitTimeout):
            mfs.wait(replay, 0.5)
        assert not mfs._tasks.build_targets
        assert mfs._tasks.namespaces["n"]["building"]["initialized"]
        print("OBSERVED accepted configuration has no tasks; identical replay is a no-op")


def test_grep_one_missing_file_aborts_other_results(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    (root / "a.txt").write_text("needle one")
    (root / "b.txt").write_text("needle two")
    from mfs import SourceUnavailable

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "external", root, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("n"), 10)
        (root / "a.txt").unlink()
        with pytest.raises(SourceUnavailable):
            mfs.grep("n", [TextMatch("needle")])
        assert mfs.read(DocumentId("n", "b.txt")).text == "needle two"
        print("OBSERVED one deleted external source aborts grep for healthy documents")


def test_plain_grep_cpu_time_exceeds_deadline(tmp_path):
    from mfs import GrepBudget

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        mfs.wait(mfs.upsert("n", "a.txt", b"needle\n" * 20000), 20)
        start = time.monotonic()
        result = mfs.grep(
            "n", [TextMatch("needle")], timeout=0.01, budget=GrepBudget(max_matches=5000)
        )
        elapsed = time.monotonic() - start
        assert result.items and elapsed > 0.25
        print(
            "OBSERVED ordinary text grep, 140 KB/20k lines/5k matches:",
            round(elapsed, 3),
            "seconds vs timeout=0.01",
        )


def test_gc_reports_normal_writer_contention_as_storage_failure(tmp_path):
    import sqlite3

    from mfs import GCPolicy

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
        assert not report.busy
        assert report.error and "database is locked" in report.error
        print("OBSERVED normal SQLite writer contention:", report)


def test_expired_query_enters_backend_after_serial_lock_releases(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
        client = mfs._runtime.index("n").client
        raw = client._client
        original = raw.search
        backend_calls = []

        def raw_search(*args, **kwargs):
            backend_calls.append(time.monotonic())
            return original(*args, **kwargs)

        def block_other_call():
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
                timed_out = time.monotonic()
                assert not backend_calls
            finally:
                release.set()
            held.result(5)
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: not mfs._tasks.queries, 5)
        assert backend_calls and backend_calls[0] > timed_out
        print("OBSERVED backend request started after caller timed out")


def test_grep_follows_replaced_source_symlink_outside_root(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    source = root / "a.txt"
    source.write_text("public original")
    outside = tmp_path / "outside.txt"
    outside.write_text("outside marker")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "external", root, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("n"), 10)
        source.unlink()
        source.symlink_to(outside)
        assert mfs.grep("n", [TextMatch("outside")]).items
        assert mfs.read(DocumentId("n", "a.txt")).text == "outside marker"
        print("OBSERVED read/grep follows a replaced source symlink outside namespace root")
