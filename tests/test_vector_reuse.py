# pyright: reportPrivateUsage=false
from __future__ import annotations

from pathlib import Path

import pytest
from test_extensions import CountingEmbedder, ReusableText

from mfs import MFS, Utf8TextProcessor
from mfs._vector_cache import VectorCache


def test_external_rename_reuses_complete_vectors_across_reopen(tmp_path: Path) -> None:
    root = tmp_path / "files"
    root.mkdir()
    source = root / "old.txt"
    source.write_text("needle")
    state = tmp_path / "state"
    processor, model = ReusableText(), CountingEmbedder()
    mfs = MFS.open(state)
    try:
        mfs.create_namespace("n", "external", root, processors=[processor], embedder=model)
        mfs.wait(mfs.sync("n"), 10)
    finally:
        mfs.close()
    source.rename(root / "new.txt")
    mfs = MFS.open(state)
    try:
        mfs.open_namespace("n", processors=[processor], embedder=model)
        report = mfs.sync("n")
        assert report.removed[0].doc_id == "old.txt"
        mfs.wait(report, 10)
        assert model.calls == 1 and processor.calls == 1
        hits = mfs.search("n", "needle", mode="vector").items
        assert len(hits) == 1 and hits[0].value.document_id.doc_id == "new.txt"
        mfs.reindex("n", timeout=10)
        assert model.calls == 2  # Explicit rebuilding invalidates the previous cache epoch.
    finally:
        mfs.close()


@pytest.mark.parametrize("cause", ["corrupt", "evict"])
def test_cache_miss_recomputes_without_changing_search_visibility(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cause: str
) -> None:
    if cause == "evict":
        monkeypatch.setattr(VectorCache, "MAX_BYTES", 250)
    model = CountingEmbedder()
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=model)
        mfs.wait(mfs.upsert("n", "a.txt", b"first needle"), 10)
        if cause == "corrupt":
            mfs._catalog.connection.execute("UPDATE vector_cache SET vector=x'00'")
        else:
            mfs.wait(mfs.upsert("n", "b.txt", b"second needle"), 10)
            assert (
                mfs._catalog.connection.execute("SELECT count(*) FROM vector_cache").fetchone()[0]
                == 1
            )
        assert mfs.search("n", "first", mode="bm25").items
        mfs.wait(mfs.remove("n", "a.txt"), 10)
        mfs.wait(mfs.upsert("n", "copy.txt", b"first needle"), 10)
        assert model.calls == (2 if cause == "corrupt" else 3)
        assert mfs.search("n", "first", mode="vector").items
    finally:
        mfs.close()
