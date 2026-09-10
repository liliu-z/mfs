# pyright: reportPrivateUsage=false
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from mfs import MFS, DocumentId, IndexFailed, StorageFailed, Utf8TextProcessor, WaitTimeout
from mfs._index import IndexRow


def test_failed_publication_keeps_new_grep_text_and_old_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state", processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal")
        mfs.upsert("n", "a.txt", b"old snapshot")
        mfs.wait_ready(10)
        original = mfs._index.replace

        def fail(identity: DocumentId, rows: Sequence[IndexRow]) -> None:
            raise IndexFailed("injected publication failure")

        monkeypatch.setattr(mfs._index, "replace", fail)
        mfs.upsert("n", "a.txt", b"new snapshot")
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs._targets[DocumentId("n", "a.txt")]["state"] == "failed", 5
            )
        assert mfs.query(select="doc").items[0].value.text == "new snapshot"
        assert mfs.search("old", mode="bm25", consistency="eventual").items
        with pytest.raises(WaitTimeout):
            mfs.search("new", mode="bm25", timeout=0)
        monkeypatch.setattr(mfs._index, "replace", original)
        mfs.retry(DocumentId("n", "a.txt"))
        mfs.wait_ready(10)
        assert mfs.search("new", mode="bm25").items
        assert not mfs.search("old", mode="bm25").items
    finally:
        mfs.close()


def test_milvus_success_before_completion_commit_replays_same_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state", processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal")
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
        assert mfs._index.count_document(DocumentId("n", "a.txt")) == 1
        assert len(mfs.search("committed", mode="bm25").items) == 1
    finally:
        mfs.close()


def test_failed_stage_survives_reopen_without_reprocessing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state"
    mfs = MFS.open(path, processors=[Utf8TextProcessor()])
    mfs.create_namespace("n", "internal")

    def fail(identity: DocumentId, rows: Sequence[IndexRow]) -> None:
        error = IndexFailed("injected publication failure")
        raise error from error  # Some backend errors contain self-referential cause chains.

    monkeypatch.setattr(mfs._index, "replace", fail)
    mfs.upsert("n", "a.txt", b"persistent")
    with mfs._condition:
        assert mfs._condition.wait_for(
            lambda: mfs._targets[DocumentId("n", "a.txt")]["state"] == "failed", 5
        )
    snapshot = mfs.query(select="doc").items[0].value.snapshot_id
    mfs.close()
    reopened = MFS.open(
        path
    )  # The already processed input needs no Processor to retry publication.
    try:
        reopened.retry(DocumentId("n", "a.txt"))
        reopened.wait_ready(10)
        assert reopened.query(select="doc").items[0].value.snapshot_id == snapshot
        assert reopened.search("persistent", mode="bm25").items
    finally:
        reopened.close()
