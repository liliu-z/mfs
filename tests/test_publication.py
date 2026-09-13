# pyright: reportPrivateUsage=false
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from mfs import MFS, DocumentId, IndexFailed, OperationFailed, StorageFailed, Utf8TextProcessor


def test_failed_publication_keeps_new_grep_text_and_revokes_old_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.upsert("n", "a.txt", b"old snapshot")
        mfs.wait_ready(10)
        original = mfs._runtime.index("n").publish

        def fail(identity: DocumentId, snapshot: str, incarnation: str, count: int) -> None:
            raise IndexFailed("injected publication failure")

        monkeypatch.setattr(mfs._runtime.index("n"), "publish", fail)
        report = mfs.upsert("n", "a.txt", b"new snapshot")
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs._tasks.targets[DocumentId("n", "a.txt")]["state"] == "failed", 5
            )
        assert mfs.grep("n", select="doc").items[0].value.text == "new snapshot"
        assert not mfs.search("n", "old", mode="bm25", consistency="eventual").items
        with pytest.raises(OperationFailed) as failed:
            mfs.search("n", "new", mode="bm25", consistency="strong", timeout=0.05)
        assert failed.value.revision == report.revision and failed.value.error_code == "IndexFailed"
        monkeypatch.setattr(mfs._runtime.index("n"), "publish", original)
        mfs.retry(DocumentId("n", "a.txt"))
        mfs.wait_ready(10)
        assert mfs.search("n", "new", mode="bm25").items
        assert not mfs.search("n", "old", mode="bm25").items
    finally:
        mfs.close()


def test_milvus_success_before_completion_commit_replays_same_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        original = mfs._catalog.put_target
        failed = False

        def fail_once(ns: str, doc: str, value: dict[str, Any]) -> None:
            nonlocal failed
            if value["state"] == "succeeded" and not failed:
                failed = True
                raise StorageFailed("injected completion failure")
            original(ns, doc, value)

        monkeypatch.setattr(mfs._catalog, "put_target", fail_once)
        mfs.upsert("n", "a.txt", b"committed text")
        mfs.wait_ready(10)
        assert failed
        assert mfs._runtime.index("n").count_document(DocumentId("n", "a.txt")) == 1
        assert len(mfs.search("n", "committed", mode="bm25").items) == 1
    finally:
        mfs.close()


def test_failed_stage_survives_reopen_without_reprocessing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state"
    mfs = MFS.open(path)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])

    def fail(identity: DocumentId, snapshot: str, incarnation: str, count: int) -> None:
        error = IndexFailed("injected publication failure")
        raise error from error  # Some backend errors contain self-referential cause chains.

    monkeypatch.setattr(mfs._runtime.index("n"), "publish", fail)
    mfs.upsert("n", "a.txt", b"persistent")
    with mfs._condition:
        assert mfs._condition.wait_for(
            lambda: mfs._tasks.targets[DocumentId("n", "a.txt")]["state"] == "failed", 5
        )
    snapshot = mfs.grep("n", select="doc").items[0].value.snapshot_id
    mfs.close()
    reopened = MFS.open(path)
    try:
        reopened.open_namespace("n", processors=[Utf8TextProcessor()])
        reopened.retry(DocumentId("n", "a.txt"))
        reopened.wait_ready(10)
        assert reopened.grep("n", select="doc").items[0].value.snapshot_id == snapshot
        assert reopened.search("n", "persistent", mode="bm25").items
    finally:
        reopened.close()
