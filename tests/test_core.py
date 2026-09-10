from __future__ import annotations

from pathlib import Path

import pytest

from mfs import (
    MFS,
    ByDocumentId,
    ByNamespace,
    Closed,
    DocumentId,
    InstanceLocked,
    TextMatch,
    UnderPath,
    Utf8TextProcessor,
)


def test_internal_lifecycle_query_filters_and_projection(tmp_path: Path) -> None:
    state = tmp_path / "state"
    mfs = MFS.open(state, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("notes", "internal")
        mfs.create_namespace("other", "internal")
        assert mfs.upsert("notes", "folder/a.md", "café\nnext".encode()).outcome == "added"
        assert mfs.upsert("notes", "folder/a.md", "café\nnext".encode()).outcome == "unchanged"
        mfs.upsert("other", "folder/a.md", b"cafe")

        mfs.wait_ready(10)
        result = mfs.query([ByNamespace("notes"), TextMatch("é")], select="doc", limit=1)
        assert result.truncated is False
        assert result.items[0].value.id == DocumentId("notes", "folder/a.md")
        assert (result.items[0].matches[0].text_start, result.items[0].matches[0].text_end) == (
            3,
            5,
        )
        assert result.items[0].matches[0].source_location.sources == (
            {"kind": "lines", "start": 1, "end": 1},
        )
        returned_source = result.items[0].matches[0].source_location.sources[0]
        assert isinstance(returned_source, dict)
        returned_source["start"] = 999
        fresh = mfs.query([ByNamespace("notes"), TextMatch("é")], select="doc")
        assert fresh.items[0].matches[0].source_location.sources[0] == {
            "kind": "lines",
            "start": 1,
            "end": 1,
        }

        combined = mfs.query([TextMatch("café"), TextMatch("\n", regex=False)], select="chunk")
        assert len(combined.items) == 1
        assert combined.items[0].value.text == "café\nnext"

        point = mfs.query([ByDocumentId(DocumentId("notes", "missing"))])
        assert point.items == ()
        assert mfs.remove("notes", "folder/a.md").outcome == "removed"
        assert mfs.remove("notes", "folder/a.md").outcome == "not_found"
    finally:
        mfs.close()


def test_processor_registry_description_is_frozen_at_open(tmp_path: Path) -> None:
    processor = Utf8TextProcessor()
    mfs = MFS.open(tmp_path / "state", processors=[processor])
    try:
        mfs.create_namespace("n", "internal")
        processor.id = "mutated"
        processor.version = "mutated"
        processor.options = {"mutated": True}
        processor.media_types = ()
        processor.suffix_media_types = {}

        assert mfs.upsert("n", "a.txt", b"frozen").outcome == "added"
        assert mfs.upsert("n", "a.txt", b"frozen").outcome == "unchanged"
    finally:
        mfs.close()


def test_bm25_order_filter_escaping_and_reopen(tmp_path: Path) -> None:
    state = tmp_path / "state"
    odd_id = 'a"\\\n%_.txt'
    mfs = MFS.open(state, processors=[Utf8TextProcessor()])
    mfs.create_namespace("n", "internal")
    mfs.upsert("n", "many.txt", b"hello hello hello")
    mfs.upsert("n", "one.txt", b"hello world")
    mfs.upsert("n", odd_id, b"needle")

    ranked = mfs.search("hello", mode="bm25", limit=10)
    assert [item.value.document_id.doc_id for item in ranked.items] == ["many.txt", "one.txt"]
    assert ranked.items[0].score > ranked.items[1].score > 0
    filtered = mfs.search("needle", filters=[ByDocumentId(DocumentId("n", odd_id))], mode="bm25")
    assert filtered.items[0].value.document_id.doc_id == odd_id
    mfs.close()

    reopened = MFS.open(state, processors=[Utf8TextProcessor()])
    try:
        assert reopened.status().document_count == 3
        assert reopened.search("needle", mode="bm25").items
    finally:
        reopened.close()


def test_external_sync_under_path_and_reconcile(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    (root / "sub").mkdir()
    (root / "a.txt").write_text("alpha")
    (root / "sub" / "b.md").write_text("beta")
    (root / "raw.bin").write_bytes(b"raw")
    state = tmp_path / "state"
    mfs = MFS.open(state, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("files", "external", root)
        report = mfs.sync("files")
        assert report.complete
        assert [item.doc_id for item in report.changed] == ["a.txt", "sub/b.md"]
        assert [(item.path, item.reason) for item in report.skipped] == [
            ("raw.bin", "unsupported_media_type")
        ]
        mfs.wait_ready(10)
        under = mfs.query([UnderPath("files", "sub")])
        assert [item.value.doc_id for item in under.items] == ["sub/b.md"]

        (root / "a.txt").unlink()
        removed = mfs.sync("files")
        assert removed.removed == (DocumentId("files", "a.txt"),)
        assert (root / "sub" / "b.md").exists()
        mfs.drop_namespace("files")
        assert (root / "sub" / "b.md").exists()
    finally:
        mfs.close()


def test_unsupported_media_on_later_sync_preserves_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    source = root / "a.txt"
    source.write_text("indexed")
    state = tmp_path / "state"
    mfs = MFS.open(state, processors=[Utf8TextProcessor()])
    mfs.create_namespace("files", "external", root)
    mfs.sync("files")
    mfs.wait_ready(10)
    mfs.close()

    source.write_text("changed but unsupported")
    without_processors = MFS.open(state)
    try:
        report = without_processors.sync("files", "a.txt")
        assert [(item.path, item.reason) for item in report.skipped] == [
            ("a.txt", "unsupported_media_type")
        ]
        document = without_processors.query(select="doc").items[0].value
        assert document.text == "indexed"
    finally:
        without_processors.close()


def test_lock_close_dirty_recovery_and_reindex(tmp_path: Path) -> None:
    state = tmp_path / "state"
    mfs = MFS.open(state, processors=[Utf8TextProcessor()])
    mfs.create_namespace("n", "internal")
    mfs.upsert("n", "a.txt", b"recover me")
    mfs.wait_ready(10)
    with pytest.raises(InstanceLocked):
        MFS.open(state, processors=[Utf8TextProcessor()])
    mfs.close()
    with pytest.raises(Closed):
        mfs.status()

    (state / "INDEX_DIRTY").write_text("1\n")
    dirty = MFS.open(state, processors=[Utf8TextProcessor()])
    try:
        dirty.wait_ready(10)
        assert dirty.status().ready
        assert dirty.query(select="doc").items[0].value.text == "recover me"
        assert dirty.search("recover", mode="bm25").items
        report = dirty.reindex()
        assert (report.documents, report.chunks) == (1, 1)
        assert dirty.search("recover", mode="bm25").items
    finally:
        dirty.close()
