# pyright: reportPrivateUsage=false
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from mfs import (
    MFS,
    ChunkRange,
    IndexFailed,
    IndexUnavailable,
    ProcessingFailed,
    SourceMap,
    StorageFailed,
    Utf8TextProcessor,
)
from mfs._index import IndexRow
from mfs._json import JSONValue


class FailingChunker:
    def __init__(self) -> None:
        self.id = "failing"
        self.version = "1"
        self.options: JSONValue = {}

    def chunk(self, text: str, source_map: SourceMap) -> Sequence[ChunkRange]:
        del text, source_map
        raise RuntimeError("injected chunk failure")


def test_index_failure_after_marker_keeps_catalog_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state"
    mfs = MFS.open(state, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal")
        mfs.upsert("n", "a.txt", b"old snapshot")
        original_replace = mfs._index.replace

        def fail_replace(document_id: object, rows: Sequence[IndexRow]) -> None:
            del document_id, rows
            raise IndexFailed("injected index failure")

        monkeypatch.setattr(mfs._index, "replace", fail_replace)
        with pytest.raises(IndexFailed, match="injected"):
            mfs.upsert("n", "a.txt", b"new snapshot")
        assert mfs.status().index_state == "dirty"
        assert mfs.query(select="doc").items[0].value.text == "old snapshot"
        with pytest.raises(IndexUnavailable):
            mfs.search("old", mode="bm25")

        monkeypatch.setattr(mfs._index, "replace", original_replace)
        mfs.reindex()
        assert mfs.search("old", mode="bm25").items
    finally:
        mfs.close()


def test_marker_cleanup_failure_commits_and_reports_not_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state"
    mfs = MFS.open(state, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal")
        original_clear = mfs._clear_marker

        def fail_clear() -> None:
            raise StorageFailed("injected cleanup failure")

        monkeypatch.setattr(mfs, "_clear_marker", fail_clear)
        report = mfs.upsert("n", "a.txt", b"committed")
        assert report.outcome == "added"
        assert not report.index_ready
        assert mfs.status().index_state == "dirty"
        assert mfs.query(select="doc").items[0].value.text == "committed"

        monkeypatch.setattr(mfs, "_clear_marker", original_clear)
        mfs.reindex()
        assert mfs.search("committed", mode="bm25").items
    finally:
        mfs.close()


def test_reindex_failure_keeps_dirty_marker(tmp_path: Path) -> None:
    state = tmp_path / "state"
    mfs = MFS.open(state, processors=[Utf8TextProcessor()])
    mfs.create_namespace("n", "internal")
    mfs.upsert("n", "a.txt", b"indexed")
    mfs.close()

    failing = MFS.open(state, processors=[Utf8TextProcessor()], chunker=FailingChunker())
    try:
        assert failing.status().index_state == "mismatch"
        with pytest.raises(ProcessingFailed, match="injected chunk failure"):
            failing.reindex()
        assert failing.status().index_state == "dirty"
        assert (state / "INDEX_DIRTY").exists()
        assert failing.query(select="doc").items[0].value.text == "indexed"
    finally:
        failing.close()
