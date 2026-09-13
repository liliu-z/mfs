from __future__ import annotations

from pathlib import Path

import pytest

from mfs import (
    MFS,
    ByDocumentId,
    Closed,
    DocumentId,
    InstanceLocked,
    TextMatch,
    UnderPath,
    Utf8TextProcessor,
)


def test_internal_lifecycle_query_filters_and_projection(tmp_path: Path) -> None:
    state = tmp_path / "state"
    mfs = MFS.open(state)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("notes", "internal", processors=[Utf8TextProcessor()])
        mfs.create_namespace("other", "internal", processors=[Utf8TextProcessor()])
        assert mfs.upsert("notes", "folder/a.md", "café\nnext".encode()).outcome == "added"
        assert mfs.upsert("notes", "folder/a.md", "café\nnext".encode()).outcome == "unchanged"
        mfs.upsert("other", "folder/a.md", b"cafe")

        mfs.wait_ready(10)
        result = mfs.grep("notes", [TextMatch("é")], select="doc", limit=1)
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
        fresh = mfs.grep("notes", [TextMatch("é")], select="doc")
        assert fresh.items[0].matches[0].source_location.sources[0] == {
            "kind": "lines",
            "start": 1,
            "end": 1,
        }

        combined = mfs.grep(
            "notes", [TextMatch("café"), TextMatch("\n", regex=False)], select="chunk"
        )
        assert len(combined.items) == 1
        assert combined.items[0].value.text == "café\nnext"

        point = mfs.grep("notes", [ByDocumentId(DocumentId("notes", "missing"))])
        assert point.items == ()
        assert mfs.remove("notes", "folder/a.md").outcome == "removed"
        assert mfs.remove("notes", "folder/a.md").outcome == "not_found"
    finally:
        mfs.close()


def test_processor_registry_description_is_frozen_at_open(tmp_path: Path) -> None:
    processor = Utf8TextProcessor()
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[processor])
    try:
        mfs.create_namespace("n", "internal", processors=[processor])
        processor.id = "mutated"
        processor.version = "mutated"
        processor.options = {"mutated": True}
        processor.media_types = ()
        processor.suffix_media_types = {}

        assert mfs.upsert("n", "a.txt", b"frozen").outcome == "added"
        assert mfs.upsert("n", "a.txt", b"frozen").outcome == "unchanged"
    finally:
        mfs.close()


def test_bm25_filter_escaping_and_reopen(tmp_path: Path) -> None:
    state = tmp_path / "state"
    odd_id = 'a"\\\n%_.txt'
    mfs = MFS.open(state)
    try:
        for registered in mfs.list_namespaces():
            mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.upsert("n", "many.txt", b"hello hello hello")
        mfs.upsert("n", "one.txt", b"hello world")
        mfs.upsert("n", odd_id, b"needle")

        ranked = mfs.search("n", "hello", mode="bm25", limit=10, consistency="strong")
        assert {item.value.document_id.doc_id for item in ranked.items} == {"many.txt", "one.txt"}
        assert all(item.score > 0 for item in ranked.items)
        filtered = mfs.search(
            "n", "needle", filters=[ByDocumentId(DocumentId("n", odd_id))], mode="bm25"
        )
        assert filtered.items[0].value.document_id.doc_id == odd_id
    finally:
        mfs.close()

    reopened = MFS.open(state)
    for registered in reopened.list_namespaces():
        reopened.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        assert reopened.status().document_count == 3
        assert reopened.search("n", "needle", mode="bm25").items
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
    mfs = MFS.open(state)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("files", "external", root, processors=[Utf8TextProcessor()])
        report = mfs.sync("files")
        assert report.complete
        assert [item.doc_id for item in report.changed] == ["a.txt", "sub/b.md"]
        assert [(item.path, item.reason) for item in report.skipped] == [
            ("raw.bin", "unsupported_media_type")
        ]
        mfs.wait_ready(10)
        under = mfs.grep("files", [UnderPath("files", "sub")])
        assert [item.value.doc_id for item in under.items] == ["sub/b.md"]

        (root / "a.txt").unlink()
        removed = mfs.sync("files")
        assert removed.removed == (DocumentId("files", "a.txt"),)
        assert (root / "sub" / "b.md").exists()
        mfs.drop_namespace("files")
        assert (root / "sub" / "b.md").exists()
    finally:
        mfs.close()


def test_unavailable_candidate_processor_preserves_serving_results(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    source = root / "a.txt"
    source.write_text("indexed")
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("files", "external", root, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("files"), 10)
        source.write_text("changed but unsupported")
        report = mfs.reprocess_namespace("files", processors=[])
        from mfs import OperationFailed

        with pytest.raises(OperationFailed):
            mfs.wait(report, 10)
        assert mfs.search("files", "indexed", mode="bm25", consistency="eventual").items
        # Only an actual source observation invalidates the serving generation.
        mfs.sync("files", verify="content")
        assert not mfs.search("files", "indexed", mode="bm25", consistency="eventual").items
    finally:
        mfs.close()


def test_lock_close_dirty_recovery_and_reindex(tmp_path: Path) -> None:
    state = tmp_path / "state"
    mfs = MFS.open(state)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
    mfs.upsert("n", "a.txt", b"recover me")
    mfs.wait_ready(10)
    with pytest.raises(InstanceLocked):
        MFS.open(state)
    mfs.close()
    with pytest.raises(Closed):
        mfs.status()

    (state / "INDEX_DIRTY").write_text("1\n")
    dirty = MFS.open(state)
    for registered in dirty.list_namespaces():
        dirty.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        dirty.wait_ready(10)
        assert dirty.status().ready
        assert dirty.grep("n", select="doc").items[0].value.text == "recover me"
        assert dirty.search("n", "recover", mode="bm25").items
        report = dirty.reindex("n")
        assert (report.documents, report.chunks) == (1, 1)
        assert dirty.search("n", "recover", mode="bm25").items
    finally:
        dirty.close()
