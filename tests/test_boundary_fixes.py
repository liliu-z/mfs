# pyright: reportPrivateUsage=false
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from mfs import (
    MFS,
    CorruptState,
    DefaultChunker,
    DocumentId,
    ExecutionPolicy,
    LocalAdmission,
    OperationFailed,
    Utf8TextProcessor,
    WaitTimeout,
)
from mfs._catalog import Catalog
from mfs._index import ChunkIndex


class Model:
    dimension = 2
    embedding_space = "boundary-model"

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        return [[1.0, 0.0] for _ in texts]

    def embed_query(self, text: str) -> list[float]:
        return [1.0, 0.0]


class FailingText(Utf8TextProcessor):
    def process(self, staged_path: Path, media_type: str) -> Any:
        if staged_path.read_bytes() == b"bad":
            raise ValueError("unreadable source")
        return super().process(staged_path, media_type)


@pytest.mark.parametrize("phase", ["catalog", "schema"])
def test_interrupted_initialization_can_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    path = tmp_path / "state"

    def fail(*args: Any, **kwargs: Any) -> None:
        raise OSError("interrupted bootstrap")

    with monkeypatch.context() as patch:
        if phase == "catalog":
            patch.setattr("mfs._core.Catalog", fail)
        else:
            patch.setattr(Catalog, "_initialize", fail)
        with pytest.raises(OSError, match="interrupted bootstrap"):
            MFS.open(path)
    with closing(MFS.open(path)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        mfs.wait(mfs.upsert("n", "a.txt", b"recovered"), 5)


def test_alias_report_waits_for_unchanged_canonical_target(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "real.txt").write_text("needle")
    (root / "alias.txt").symlink_to("real.txt")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n",
            "external",
            root,
            processors=[Utf8TextProcessor()],
            indexing="off",
            processing_paused=True,
        )
        first = mfs.sync("n", "alias.txt")
        second = mfs.sync("n", "alias.txt")
        assert first.changed == (DocumentId("n", "real.txt"),)
        assert second.changed == ()
        for report in (first, second):
            with pytest.raises(WaitTimeout):
                mfs.wait(report, 0.05)
        mfs.configure_processing("n", paused=False)
        mfs.wait(second, 5)


def test_unbound_pending_explains_wait_and_recovers_on_binding(tmp_path: Path) -> None:
    path = tmp_path / "state"
    with closing(MFS.open(path, start_paused=True)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        accepted = mfs.upsert("n", "a.txt", b"needle")
    with closing(MFS.open(path)) as mfs:
        status = mfs.document_status(accepted.id)
        assert status is not None
        assert status.blocking_reason == "binding"
        with pytest.raises(OperationFailed, match="binding") as failed:
            mfs.wait(accepted, 0.1)
        assert failed.value.state == "blocked"
        mfs.open_namespace("n", processors=[Utf8TextProcessor()])
        mfs.wait(accepted, 5)
        assert mfs.document_status(accepted.id).blocking_reason is None  # type: ignore[union-attr]


def test_binding_status_does_not_depend_on_worker_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from mfs._worker import Worker

    release = threading.Event()
    original = Worker.run

    def slow_start(worker: Worker) -> None:
        assert release.wait(15)
        original(worker)

    monkeypatch.setattr(Worker, "run", slow_start)
    with closing(MFS.open(tmp_path / "state")) as mfs:
        try:
            mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
            report = mfs.upsert("n", "a.txt", b"ready binding")
            status = mfs.document_status(report.id)
            assert status is not None and status.blocking_reason is None
            with pytest.raises(WaitTimeout):
                mfs.wait(report, 0.05)
        finally:
            release.set()
        mfs.wait(report, 5)


def test_drop_settles_exhausted_cleanup_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state"
    with closing(MFS.open(path)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"old needle"), 10)
        with mfs._condition:
            mfs.upsert("n", "a.txt", b"new needle")
            # Persist the exact terminal state produced by five cleanup failures.
            rows = mfs._catalog.cleanup_rows("n")
            assert rows
            mfs._catalog.execute(
                "UPDATE index_cleanup SET value=json_set(value,'$.failures',5,'$.error',?)",
                ("backend outage",),
            )
        dropped = mfs.drop_namespace("n")
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs._tasks.targets[DocumentId("n", "")]["state"] == "succeeded", 10
            )
        mfs.wait(dropped, 0.1)
        assert mfs._catalog.cleanup_rows("n") == []
    with closing(MFS.open(path)) as mfs:
        mfs.wait(dropped, 0.1)
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"fresh needle"), 10)
        assert len(mfs.search("n", "needle", mode="bm25").items) == 1


def test_reopen_settles_debt_left_by_an_older_successful_drop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state"
    with closing(MFS.open(state)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        accepted = mfs.upsert("n", "a.txt", b"needle")
        mfs.wait(accepted, 10)
        with mfs._condition, mfs._catalog.transaction():
            mfs._catalog.enqueue_cleanup("n", "a.txt", mfs._tasks.targets[accepted.id])
            mfs._catalog.execute(
                "UPDATE index_cleanup SET value=json_set(value,'$.failures',5,'$.error','outage')"
            )

        def old_completion(incarnation: str, generation: str | None) -> None:
            pass

        monkeypatch.setattr(mfs._catalog, "settle_collection", old_completion)
        dropped = mfs.drop_namespace("n")
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs._tasks.targets[DocumentId("n", "")]["state"] == "succeeded", 10
            )
        assert mfs._catalog.cleanup_rows("n")
        assert mfs._runtime.legacy_index.client.list_collections() == []
    with closing(MFS.open(state)) as mfs:
        mfs.wait(dropped, 0.1)
        assert mfs._catalog.cleanup_rows("n") == []


def test_failed_member_does_not_block_first_vector_publication(tmp_path: Path) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[FailingText()])
        good = mfs.upsert("n", "good.txt", b"needle")
        mfs.wait(good, 10)
        bad = mfs.upsert("n", "bad.txt", b"bad")
        with pytest.raises(OperationFailed):
            mfs.wait(bad, 10)
        mfs.configure_namespace("n", embedder=Model(), indexing="hybrid")
        until = time.monotonic() + 10
        while mfs.namespace_configuration("n").pending_revision is not None:
            assert time.monotonic() < until, "failed member prevented publication"
            time.sleep(0.02)
        assert mfs.namespace_configuration("n").indexing == "hybrid"
        assert [
            hit.value for hit in mfs.search("n", "needle", mode="vector", select="doc_id").items
        ] == [good.id]
        failed = mfs.document_status(bad.id)
        assert failed is not None and failed.state == "failed"
        with pytest.raises(OperationFailed):
            mfs.wait(bad, 0.1)


def test_slow_collection_maintenance_does_not_occupy_other_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    original = ChunkIndex.recreate

    class Chunker(DefaultChunker):
        def __init__(self) -> None:
            super().__init__()
            self.version = "boundary-2"

    with closing(MFS.open(tmp_path / "state", execution=ExecutionPolicy(stage_timeout=10))) as mfs:
        for namespace in ("a", "b"):
            mfs.create_namespace(
                namespace, "internal", processors=[Utf8TextProcessor()], indexing="off"
            )
        incarnation = mfs._tasks.namespaces["a"]["incarnation"]

        def slow(index: ChunkIndex, **kwargs: Any) -> None:
            if incarnation in index.collection_name:
                entered.set()
                assert release.wait(20)
            original(index, **kwargs)

        monkeypatch.setattr(ChunkIndex, "recreate", slow)
        try:
            a = mfs.configure_namespace("a", chunker=Chunker())
            assert entered.wait(5)
            b = mfs.configure_namespace("b", chunker=Chunker())
            mfs.wait(b, 5)
            assert mfs.namespace_configuration("a").pending_revision is not None
        finally:
            release.set()
        mfs.wait(a, 10)


@pytest.mark.skipif(os.name != "posix", reason="SIGKILL crash injection")
@pytest.mark.parametrize("phase", ["lock", "catalog", "schema"])
def test_bootstrap_sigkill_recovery(tmp_path: Path, phase: str) -> None:
    code = """
import os, signal, sys
from pathlib import Path
from unittest.mock import patch
from mfs import MFS
def crash(*args, **kwargs):
    os.kill(os.getpid(), signal.SIGKILL)
target = {"lock": "mfs._core.FileLock", "catalog": "mfs._core.Catalog",
          "schema": "mfs._catalog.Catalog._initialize"}[sys.argv[2]]
with patch(target, side_effect=crash):
    MFS.open(Path(sys.argv[1]))
"""
    state = tmp_path / "state"
    child = subprocess.run(
        [sys.executable, "-c", code, str(state), phase], capture_output=True, timeout=15
    )
    assert child.returncode == -9, child.stderr.decode()
    with closing(MFS.open(state)) as mfs:
        assert mfs.status().ready


def test_bootstrap_does_not_adopt_unrelated_files(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    source = state / "user.txt"
    source.write_text("keep")
    for marked in (False, True):
        if marked:
            (state / ".mfs-initializing").mkdir()
        with pytest.raises(CorruptState):
            MFS.open(state)
        assert source.read_text() == "keep"
        assert not (state / "catalog.sqlite").exists()
        assert not (state / "LOCK").exists()


@pytest.mark.parametrize("version", [0, -1])
def test_bootstrap_rejects_foreign_schema_without_migrating_it(
    tmp_path: Path, version: int
) -> None:
    state = tmp_path / "state"
    state.mkdir()
    (state / ".mfs-initializing").mkdir()
    catalog = state / "catalog.sqlite"
    with closing(sqlite3.connect(catalog)) as connection:
        connection.execute("CREATE TABLE user_data(value TEXT)")
        connection.execute("INSERT INTO user_data VALUES('keep')")
        connection.execute(f"PRAGMA user_version={version}")
        connection.commit()
    with pytest.raises(CorruptState):
        MFS.open(state)
    with closing(sqlite3.connect(catalog)) as connection:
        assert connection.execute("SELECT * FROM user_data").fetchall() == [("keep",)]
        assert connection.execute("PRAGMA user_version").fetchone() == (version,)


@pytest.mark.parametrize("redirect_root", [False, True])
def test_sync_report_covers_targets_outside_requested_subtree(
    tmp_path: Path, redirect_root: bool
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "sub").mkdir()
    (root / "outside.txt").write_text("outside")
    (root / "sub" / "alias.txt").symlink_to("../outside.txt")
    link = tmp_path / "link"
    link.symlink_to(root, target_is_directory=True)
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n",
            "external",
            link,
            processors=[Utf8TextProcessor()],
            indexing="off",
            processing_paused=True,
        )
        if redirect_root:
            replacement = tmp_path / "replacement"
            replacement.mkdir()
            (replacement / "outside.txt").write_text("replacement")
            link.unlink()
            link.symlink_to(replacement, target_is_directory=True)
        report = mfs.sync("n", "sub")
        assert report.complete
        if redirect_root:
            assert report.path == "."
        else:
            assert report.wait_paths == ("outside.txt",)
        with pytest.raises(WaitTimeout):
            mfs.wait(report, 0.05)
        mfs.configure_processing("n", paused=False)
        mfs.wait(report, 5)


def test_drop_waits_for_late_writer_before_settling_its_debt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    original = ChunkIndex.insert
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        incarnation = mfs._tasks.namespaces["n"]["incarnation"]

        def slow(index: ChunkIndex, *args: Any, **kwargs: Any) -> Any:
            if incarnation in index.collection_name:
                entered.set()
                assert release.wait(15)
            return original(index, *args, **kwargs)

        monkeypatch.setattr(ChunkIndex, "insert", slow)
        try:
            mfs.upsert("n", "old.txt", b"old needle")
            assert entered.wait(5)
            with mfs._condition, mfs._catalog.transaction():
                mfs._catalog.enqueue_cleanup(
                    "n", "old.txt", mfs._tasks.targets[DocumentId("n", "old.txt")]
                )
                mfs._catalog.execute(
                    "UPDATE index_cleanup SET "
                    "value=json_set(value,'$.failures',5,'$.error','outage')"
                )
            drop = mfs.drop_namespace("n")
            with pytest.raises(WaitTimeout):
                mfs.wait(drop, 0.05)
            assert mfs._catalog.cleanup_rows("n")
            mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
            fresh = mfs.upsert("n", "new.txt", b"new needle")
            mfs.wait(fresh, 5)
            assert not mfs.document_status(fresh.id).cleanup_pending  # type: ignore[union-attr]
        finally:
            release.set()
        mfs.wait(drop, 10)
        assert mfs._catalog.cleanup_rows("n") == []
        assert [h.value for h in mfs.search("n", "needle", mode="bm25", select="doc_id").items] == [
            fresh.id
        ]


def test_partial_model_replacement_preserves_failures_cancellation_and_retry_on_reopen(
    tmp_path: Path,
) -> None:
    class NewModel(Model):
        dimension = 3
        embedding_space = "boundary-new"

        def __init__(self, fail: bool) -> None:
            self.fail = fail

        def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
            if self.fail and any("bad" in text for text in texts):
                raise ValueError("model rejected document")
            return [[1.0, 0.0, 0.0] for _ in texts]

        def embed_query(self, text: str) -> list[float]:
            return [1.0, 0.0, 0.0]

    state = tmp_path / "state"
    good, bad, cancelled = [DocumentId("n", f"{name}.txt") for name in ("good", "bad", "cancelled")]
    with closing(MFS.open(state)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=Model())
        for identity in (good, bad, cancelled):
            mfs.wait(mfs.upsert("n", identity.doc_id, identity.doc_id.encode()), 10)
        mfs.configure_processing("n", paused=True)
        change = mfs.configure_namespace("n", embedder=NewModel(True))
        with mfs._condition:
            assert mfs._condition.wait_for(lambda: cancelled in mfs._tasks.build_targets, 5)
        mfs.cancel(cancelled)
        mfs.configure_processing("n", paused=False)
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs.namespace_configuration("n").active_revision == change.revision, 10
            )
        assert [h.value for h in mfs.search("n", "q", mode="vector", select="doc_id").items] == [
            good
        ]
        assert mfs.document_status(bad).state == "failed"  # type: ignore[union-attr]
        assert mfs.document_status(cancelled).state == "cancelled"  # type: ignore[union-attr]
        assert mfs.read(cancelled) is not None
    with closing(MFS.open(state)) as mfs:
        mfs.open_namespace("n", processors=[Utf8TextProcessor()], embedder=NewModel(False))
        assert mfs.document_status(bad).state == "failed"  # type: ignore[union-attr]
        assert mfs.document_status(cancelled).state == "cancelled"  # type: ignore[union-attr]
        mfs.retry(bad)
        mfs.wait(bad, 10)
        assert {h.value for h in mfs.search("n", "q", mode="vector", select="doc_id").items} == {
            good,
            bad,
        }
        assert mfs.document_status(cancelled).state == "cancelled"  # type: ignore[union-attr]
        mfs.retry(cancelled)
        mfs.wait("n", 10)
        assert len(mfs.search("n", "q", mode="vector").items) == 3


def test_pending_distinguishes_pause_resources_and_actual_retirement(tmp_path: Path) -> None:
    admission = LocalAdmission({"light": 1, "heavy": 1})
    held = admission.try_acquire({"light": 1})
    assert held is not None
    try:
        with closing(MFS.open(tmp_path / "state", admission=admission)) as mfs:
            mfs.create_namespace(
                "n",
                "internal",
                processors=[Utf8TextProcessor()],
                indexing="off",
                processing_paused=True,
            )
            report = mfs.upsert("n", "a.txt", b"text")
            assert mfs.document_status(report.id).blocking_reason == "processing_paused"  # type: ignore[union-attr]
            mfs.configure_processing("n", paused=False)
            with mfs._condition:
                assert mfs._condition.wait_for(
                    lambda: (
                        (status := mfs.document_status(report.id)) is not None
                        and status.blocking_reason == "resources"
                    ),
                    5,
                )
            held.release()
            mfs.wait(report, 5)
            assert mfs.document_status(report.id).blocking_reason is None  # type: ignore[union-attr]
    finally:
        held.release()


@pytest.mark.conformance
def test_real_lite_ddl_timeout_retains_owner_and_other_collection_can_search(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from milvus_lite.db import MilvusLite  # pyright: ignore[reportMissingTypeStubs]

    entered, release = threading.Event(), threading.Event()
    original = MilvusLite.create_collection
    blocked_incarnation = "never-match"

    def slow(db: Any, name: str, *args: Any, **kwargs: Any) -> Any:
        if blocked_incarnation in name:
            entered.set()
            assert release.wait(20)
        return original(db, name, *args, **kwargs)

    monkeypatch.setattr(MilvusLite, "create_collection", slow)
    with closing(MFS.open(tmp_path / "state", execution=ExecutionPolicy(stage_timeout=3))) as mfs:
        for namespace in ("a", "b"):
            mfs.create_namespace(namespace, "internal", processors=[Utf8TextProcessor()])
            mfs.wait(mfs.upsert(namespace, "a.txt", b"needle"), 10)
        blocked_incarnation = mfs._tasks.namespaces["a"]["incarnation"]
        try:
            change = mfs.configure_namespace("a", embedder=Model(), indexing="hybrid")
            assert entered.wait(5)
            assert mfs.search("b", "needle", mode="bm25", timeout=1).items
            with mfs._condition:
                assert mfs._condition.wait_for(
                    lambda: mfs.namespace_configuration("a").pending_failures >= 5, 5
                )
                assert change.revision in mfs._configuration.creating
                assert mfs._tasks.namespace_executions[blocked_incarnation] > 0
            with pytest.raises(OperationFailed, match="timeout"):
                mfs.wait(change, 0.1)
            dropped = mfs.drop_namespace("a")
            with pytest.raises(WaitTimeout):
                mfs.wait(dropped, 0.1)
            assert mfs.search("b", "needle", mode="bm25", timeout=1).items
        finally:
            release.set()
        mfs.wait(dropped, 10)
        assert all(
            blocked_incarnation not in name
            for name in mfs._runtime.legacy_index.client.list_collections()
        )


def test_waiting_configuration_does_not_spin_maintenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], processing_paused=True
        )
        mfs.upsert("n", "pending.txt", b"pending")
        mfs.configure_namespace("n", embedder=Model(), indexing="hybrid")
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs._tasks.namespaces["n"].get("building", {}).get("initialized"), 5
            )
        original = mfs._configuration.maintain
        calls = 0

        def count(namespace: str) -> None:
            nonlocal calls
            calls += 1
            original(namespace)

        monkeypatch.setattr(mfs._configuration, "maintain", count)
        time.sleep(0.2)
        assert calls < 20, f"paused configuration spun {calls} maintenance passes in 0.2s"
