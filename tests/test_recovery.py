# pyright: reportPrivateUsage=false
from __future__ import annotations

import subprocess
import sys
import threading
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import pytest

from mfs import MFS, DocumentId, GCPolicy, StorageFailed, Utf8TextProcessor, WaitTimeout


@pytest.mark.parametrize(
    "boundary",
    ["before_accept_commit", "after_accept_commit", "after_process_artifact", "after_publish"],
)
def test_process_termination_at_durable_boundaries(tmp_path: Path, boundary: str) -> None:
    state = tmp_path / "state"
    script = """
import os, sys
from pathlib import Path
from mfs import MFS, Utf8TextProcessor
state, boundary = Path(sys.argv[1]), sys.argv[2]
mfs = MFS.open(state)
for registered in mfs.list_namespaces():
    mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
mfs.create_namespace('n', 'internal', processors=[Utf8TextProcessor()])
def stop():
    os._exit(23)
if boundary == "before_accept_commit":
    original = mfs._catalog.put_target
    def accept(*args):
        original(*args)
        stop()  # The surrounding SQLite transaction has not committed.
    mfs._catalog.put_target = accept
elif boundary == "after_accept_commit":
    mfs._tasks.remember = lambda *args: stop()  # Accepted on disk, before memory publication / ACK.
elif boundary == "after_process_artifact":
    original = mfs._write_artifact
    def artifact(name, value, incarnation):
        result = original(name, value, incarnation)
        if name.endswith("-snapshot"):
            stop()
        return result
    mfs._write_artifact = artifact
elif boundary == "after_publish":
    original = mfs._namespace_index("n").publish
    def publish(*args):
        original(*args)
        stop()
    mfs._namespace_index("n").publish = publish
mfs.upsert("n", "a.txt", b"durable needle", idempotency_key="request-1")
mfs.wait_ready(10)
raise AssertionError("crash boundary was not reached")
"""
    child = subprocess.run(
        [sys.executable, "-c", script, str(state), boundary], capture_output=True, timeout=25
    )
    assert child.returncode == 23, child.stderr.decode()
    # Binding validates the manifest; successful PROCESS output is still reused.
    processors = [Utf8TextProcessor()]
    mfs = MFS.open(state, gc_policy=GCPolicy(enabled=False, idle_seconds=0, grace_seconds=0))
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=processors)
    try:
        mfs.wait_ready(10)
        if boundary == "before_accept_commit":
            assert not mfs.grep().items
            for _ in range(3):
                assert mfs.collect_garbage().error is None
            assert not list((state / "namespaces").glob("*/originals/*"))
        else:
            assert mfs.grep(select="doc").items[0].value.text == "durable needle"
            assert len(mfs.search("needle", mode="bm25").items) == 1
            assert mfs._namespace_index("n").count_document(DocumentId("n", "a.txt")) == 1
    finally:
        mfs.close()


def test_lost_accept_ack_reconciles_live_pending_and_replays_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        original = mfs._catalog.transaction
        failed = False

        @contextmanager
        def commit_then_fail() -> Generator[None]:
            nonlocal failed
            with original():
                yield
            if not failed:
                failed = True
                raise StorageFailed("injected ACK loss after commit")

        monkeypatch.setattr(mfs._catalog, "transaction", commit_then_fail)
        with pytest.raises(StorageFailed):
            mfs.upsert("n", "a.txt", b"accepted", idempotency_key="request")
        receipt = mfs.upsert("n", "a.txt", b"accepted", idempotency_key="request")
        assert receipt.outcome == "added"
        mfs.wait_ready(10)
        assert mfs.search("accepted", mode="bm25").items
    finally:
        mfs.close()


def test_namespace_recreation_while_old_publication_runs_preserves_new_incarnation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    entered, release = threading.Event(), threading.Event()
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        original = mfs._namespace_index("n").publish
        first = True

        def delayed(identity: DocumentId, snapshot: str, incarnation: str, count: int) -> None:
            nonlocal first
            if first:
                first = False
                entered.set()
                assert release.wait(10)
            original(identity, snapshot, incarnation, count)

        monkeypatch.setattr(mfs._namespace_index("n"), "publish", delayed)
        mfs.upsert("n", "old.txt", b"old content")
        assert entered.wait(5)
        mfs.drop_namespace("n")
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.upsert("n", "new.txt", b"new content")
        assert not mfs.status().ready
        release.set()
        mfs.wait_ready(10)
        assert not mfs.search("old", mode="bm25").items
        assert mfs.search("new", mode="bm25").items[0].value.document_id.doc_id == "new.txt"
    finally:
        release.set()
        mfs.close()


def test_search_does_not_hold_publication_lock_after_strong_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    entered, release = threading.Event(), threading.Event()
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.upsert("n", "a.txt", b"old content")
        mfs.wait_ready(10)
        original = mfs._wait_ready

        def admitted(timeout: float | None, namespaces: set[str] | None = None) -> None:
            original(timeout, namespaces)
            entered.set()
            assert release.wait(10)

        monkeypatch.setattr(mfs, "_wait_ready", admitted)
        with ThreadPoolExecutor() as pool:
            future = pool.submit(mfs.search, "new", mode="bm25")
            try:
                assert entered.wait(5)
                mfs.upsert("n", "a.txt", b"new content")
                original(5)  # The new write must finish while the admitted reader is paused.
                release.set()
                assert future.result(5).items[0].value.text == "new content"
            finally:
                release.set()
    finally:
        release.set()
        mfs.close()


def test_pending_delete_keeps_ready_false_and_revokes_old_hit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    entered, release = threading.Event(), threading.Event()
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        inserted = mfs.upsert("n", "a.txt", b"old content")
        mfs.wait_ready(10)
        original = mfs._namespace_index("n").delete_document

        def delayed(identity: DocumentId, *, incarnation: str | None = None) -> None:
            entered.set()
            assert release.wait(10)
            original(identity, incarnation=incarnation)

        monkeypatch.setattr(mfs._namespace_index("n"), "delete_document", delayed)
        removed = mfs.remove("n", "a.txt")
        assert entered.wait(5)
        status = mfs.document_status(inserted.id)
        assert status and status.text_revision is None and status.indexed_revision is None
        assert not mfs.grep().items
        assert not mfs.search("old", mode="bm25", consistency="eventual").items
        with pytest.raises(WaitTimeout):
            mfs.wait_ready(0)
        release.set()
        mfs.wait(removed, 10)
        status = mfs.document_status(inserted.id)
        assert status and status.indexed_revision is None
        assert not mfs.search("old", mode="bm25").items
    finally:
        release.set()
        mfs.close()


def test_v1_catalog_requires_explicit_migration_with_adapters(tmp_path: Path) -> None:
    import json
    import sqlite3

    from mfs import MigrationRequired

    path = tmp_path / "state"
    mfs = MFS.open(path)
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"migrated needle"), 10)
        mfs._namespace_index("n").drop()
    finally:
        mfs.close()
    # A v1 document held an owned original and inline processed text, without targets.
    with sqlite3.connect(path / "catalog.sqlite") as connection:
        connection.execute("DROP TABLE targets")
        connection.execute("DROP TABLE operations")
        connection.execute("PRAGMA user_version=1")
        for table in ("namespaces", "documents"):
            for rowid, encoded in connection.execute(f"SELECT rowid,value FROM {table}"):
                value = json.loads(encoded)
                value["version"] = 1
                for name in (
                    "incarnation",
                    "revision",
                    "binding",
                    "root_actual",
                    "manifest",
                    "indexing",
                    "rules",
                    "rules_revision",
                    "text_ref",
                ):
                    value.pop(name, None)
                if table == "documents":
                    value["text"] = "stale legacy text"
                connection.execute(
                    f"UPDATE {table} SET value=? WHERE rowid=?", (json.dumps(value), rowid)
                )
    reopened = MFS.open(path)
    try:
        with pytest.raises(MigrationRequired):
            reopened.grep()
        reopened.migrate_namespace("n", processors=[Utf8TextProcessor()], indexing="bm25")
        reopened.wait_ready(10)
        assert reopened.grep(select="doc").items[0].value.text == "migrated needle"
        assert reopened.search("needle", mode="bm25").items
        assert not reopened.search("stale", mode="bm25").items
        record = reopened._catalog.get_document("n", "a.txt")
        assert record is not None and "text" not in record
        status = reopened.document_status(DocumentId("n", "a.txt"))
        assert status is not None and status.text_revision == status.indexed_revision
    finally:
        reopened.close()


def test_reindex_retries_failed_index_target_and_status_does_not_hydrate_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_lifecycle import GateEmbedder, wait_state

    embedder = GateEmbedder()
    embedder.fail = True
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(
            registered.namespace, processors=[Utf8TextProcessor()], embedder=embedder
        )
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=embedder)
        mfs.upsert("n", "a.txt", b"rebuild needle")
        identity = DocumentId("n", "a.txt")
        wait_state(mfs, identity, "failed")

        def no_hydration(namespace: str, doc_id: str) -> None:
            raise AssertionError("status must not load the document body")

        with monkeypatch.context() as check:
            check.setattr(mfs._catalog, "get_document", no_hydration)
            assert mfs.document_status(identity) is not None
        embedder.fail = False
        assert mfs.reindex("n", timeout=10).documents == 1
        assert mfs.search("needle", mode="bm25").items
    finally:
        mfs.close()


def test_namespace_cleanup_failure_is_visible_and_retryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_lifecycle import wait_state

    from mfs import IndexFailed

    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.upsert("n", "a.txt", b"old needle")
        mfs.wait_ready(10)
        old_index = mfs._namespace_index("n")
        original = old_index.drop

        def failing() -> None:
            raise IndexFailed("injected cleanup failure")

        monkeypatch.setattr(old_index, "drop", failing)
        mfs.drop_namespace("n")
        identity = DocumentId("n", "")
        wait_state(mfs, identity, "failed")
        tasks = mfs.list_document_statuses()
        assert len(tasks) == 1 and tasks[0].stage == "drop" and tasks[0].error
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.upsert("n", "new.txt", b"new content")
        monkeypatch.setattr(old_index, "drop", original)
        mfs.retry(tasks[0].id)
        mfs.wait_ready(10)
        assert not mfs.search("old", mode="bm25").items
        assert mfs.search("new", mode="bm25").items
    finally:
        mfs.close()
