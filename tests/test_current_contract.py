# pyright: reportPrivateUsage=false
from __future__ import annotations

import io
import threading
import zipfile
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

from mfs import (
    MFS,
    DocumentId,
    DocxProcessor,
    GCPolicy,
    GrepBudget,
    OperationFailed,
    ProcessedDocument,
    SourceMap,
    StorageFailed,
    TextMatch,
    Utf8TextProcessor,
)
from mfs._json import JSONValue


def test_docx_extraction_uses_one_derived_text_and_paragraph_locations(tmp_path: Path) -> None:
    source = io.BytesIO()
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            "word/document.xml",
            '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
            "<w:body><w:p><w:r><w:t>文档 needle</w:t></w:r></w:p>"
            "<w:tbl><w:tr><w:tc><w:p><w:r><w:t>table content</w:t></w:r></w:p>"
            "</w:tc></w:tr></w:tbl></w:body></w:document>",
        )
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[DocxProcessor()])
        mfs.wait(mfs.upsert("n", "a.docx", source.getvalue()), 10)
        hit = mfs.search("n", "needle", mode="bm25").items[0].value
        assert hit.text == "文档 needle\ntable content\n"
        assert hit.source_location.sources[0]["kind"] == "paragraphs"
        assert len(list((tmp_path / "state/namespaces").glob("*/derived/*.md"))) == 1
    finally:
        mfs.close()


class HtmlProcessor:
    id = "html"
    version = "1"
    options: JSONValue = None
    media_types: tuple[str, ...] = ("text/html",)
    suffix_media_types: Mapping[str, str] = MappingProxyType({".html": "text/html"})

    def sniff(self, head: bytes) -> str | None:
        return None

    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
        return ProcessedDocument("visible needle", SourceMap(1, ()), grep_path=staged_path)


def test_html_greps_source_and_indexes_transient_extraction_across_reopen(tmp_path: Path) -> None:
    root = tmp_path / "files"
    root.mkdir()
    (root / "a.html").write_text('<html data-hidden="markup"><body>visible needle</body></html>')
    state = tmp_path / "state"
    for iteration in range(2):
        mfs = MFS.open(state)
        try:
            if iteration == 0:
                mfs.create_namespace("n", "external", root, processors=[HtmlProcessor()])
                mfs.wait(mfs.sync("n"), 10)
            else:
                mfs.open_namespace("n", processors=[HtmlProcessor()])
                mfs.reindex("n", timeout=10)
            assert mfs.grep("n", [TextMatch("markup")]).items
            assert not mfs.search("n", "markup", mode="bm25").items
            assert mfs.search("n", "needle", mode="bm25").items[0].value.text == "visible needle"
            for file in (state / "namespaces").rglob("*"):
                if file.is_file():
                    assert b"visible needle" not in file.read_bytes()
        finally:
            mfs.close()


def test_reprocess_transaction_rolls_back_and_keeps_owned_source_for_missing_route(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(
        tmp_path / "state", gc_policy=GCPolicy(enabled=False, idle_seconds=0, grace_seconds=0)
    )
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"owned needle"), 10)

        def fail(*args: Any) -> None:
            raise StorageFailed("injected target failure")

        with monkeypatch.context() as check:
            check.setattr(mfs._catalog, "put_target", fail)
            with pytest.raises(StorageFailed):
                mfs.reprocess_namespace("n", processors=[])
        mfs.open_namespace("n", processors=[Utf8TextProcessor()])
        assert mfs.search("n", "needle", mode="bm25").items
        reports = mfs.reprocess_namespace("n", processors=[])
        assert isinstance(reports, tuple)
        with pytest.raises(OperationFailed):
            mfs.wait(reports[0], 10)
        assert not mfs.grep("n").items
        assert not mfs.search("n", "needle", mode="bm25", consistency="eventual").items
        for _ in range(3):
            assert mfs.collect_garbage().error is None
        restored = mfs.reprocess_namespace("n", processors=[Utf8TextProcessor()])
        assert isinstance(restored, tuple)
        mfs.wait(restored[0], 10)
        document = mfs.read(DocumentId("n", "a.txt"))
        assert document is not None and document.original == b"owned needle"
    finally:
        mfs.close()


def test_scoped_search_and_receipts_work_without_other_namespace_binding(tmp_path: Path) -> None:
    state = tmp_path / "state"
    mfs = MFS.open(state)
    try:
        for name in ("a", "b"):
            mfs.create_namespace(name, "internal", processors=[Utf8TextProcessor()])
            mfs.wait(mfs.upsert(name, "a.txt", b"needle"), 10)
    finally:
        mfs.close()
    mfs = MFS.open(state)
    try:
        mfs.open_namespace("a", processors=[Utf8TextProcessor()])
        pending = mfs.upsert("b", "a.txt", b"new needle")
        assert mfs.search("a", "needle", mode="bm25", consistency="strong", timeout=5).items
        repeats = [mfs.upsert("b", "a.txt", b"new needle") for _ in range(3)]
        assert all(r.revision == pending.revision for r in repeats)
        assert (
            mfs._catalog.connection.execute(
                "SELECT count(*) FROM targets WHERE namespace='b' AND doc_id='a.txt'"
            ).fetchone()[0]
            == 1
        )
        tables = {
            r[0]
            for r in mfs._catalog.connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='table'"
            )
        }
        assert not tables.intersection(
            {"runs", "wait_operations", "wait_target_sets", "run_dependencies"}
        )
        mfs.open_namespace("b", processors=[Utf8TextProcessor()])
        for receipt in [pending, *repeats]:
            mfs.wait(receipt, 10)
        assert [s.id.namespace for s in mfs.list_document_statuses(limit=1, offset=1)] == ["b"]
        assert mfs.scope_status("b").total == 1
    finally:
        mfs.close()


def test_grep_does_not_return_revoked_text_after_slow_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from mfs._reader import Reader

    mfs = MFS.open(tmp_path / "state")
    entered, release = threading.Event(), threading.Event()
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"old needle"), 10)
        original = Reader._read

        def slow(owner: Reader, record: dict[str, Any], maximum: int) -> tuple[str, bool, int]:
            result = original(owner, record, maximum)
            entered.set()
            assert release.wait(5)
            return result

        monkeypatch.setattr(Reader, "_read", slow)
        with ThreadPoolExecutor() as pool:
            future = pool.submit(mfs.grep, "n", [TextMatch("old")])
            try:
                assert entered.wait(5)
                mfs.remove("n", "a.txt")
                release.set()
                assert not future.result(5).items
            finally:
                release.set()
    finally:
        release.set()
        mfs.close()


def test_grep_whole_word_budget_skips_partial_words(tmp_path: Path) -> None:
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"needles needles needles needle"), 10)
        result = mfs.grep(
            "n", [TextMatch("needle", whole_word=True)], budget=GrepBudget(max_matches=1)
        )
        assert len(result.items) == 1 and len(result.items[0].matches) == 1
    finally:
        mfs.close()


def test_cancelled_cleanup_retry_keeps_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"old needle"), 10)
        mfs.configure_index("n", paused=True)
        with mfs._condition:
            receipt = mfs.upsert("n", "a.txt", b"new needle")
            mfs.cancel(receipt.id)
            original = mfs._runtime.index("n").delete_document
            calls = 0

            def temporary(identity: DocumentId, *, incarnation: str | None = None) -> None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise OSError("retry the physical cleanup")
                original(identity, incarnation=incarnation)

            monkeypatch.setattr(mfs._runtime.index("n"), "delete_document", temporary)
            assert mfs._condition.wait_for(
                lambda: calls == 2 and not mfs._tasks.targets[receipt.id].get("cleanup"), 10
            )
            assert mfs._tasks.targets[receipt.id]["state"] == "cancelled"
        assert not mfs.grep("n").items
        assert not mfs.search("n", "old", mode="bm25", consistency="eventual").items
        mfs.retry(receipt.id)
        mfs.configure_index("n", paused=False)
        mfs.wait(receipt, 10)
        assert mfs.search("n", "new", mode="bm25").items
    finally:
        mfs.close()


def test_legacy_text_processor_reuses_external_input_without_copy(tmp_path: Path) -> None:
    from dataclasses import replace

    class LegacyText(Utf8TextProcessor):
        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            return replace(super().process(staged_path, media_type), text_path=None)

    root = tmp_path / "files"
    root.mkdir()
    (root / "a.txt").write_text("existing plain text")
    state = tmp_path / "state"
    mfs = MFS.open(state)
    try:
        mfs.create_namespace("n", "external", root, processors=[LegacyText()])
        mfs.wait(mfs.sync("n"), 10)
        assert not list((state / "namespaces").glob("*/derived/*.md"))
        (root / "a.txt").write_text("changed live text")
        assert mfs.grep("n", [TextMatch("changed")]).items
    finally:
        mfs.close()
