# pyright: reportPrivateUsage=false
from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal

import pytest
from test_lifecycle import ByteChunker, GateEmbedder

from mfs import (
    MFS,
    AnyOf,
    ByDocumentId,
    ByExtension,
    ByMediaType,
    DocumentId,
    Filter,
    InvalidFilter,
    InvalidPattern,
    InvalidQuery,
    NamePrefix,
    NameSuffix,
    PathPrefix,
    PathSuffix,
    ProcessedDocument,
    SourceMap,
    SourceSpan,
    TextMatch,
    UnderPath,
    Utf8TextProcessor,
)
from mfs._filters import compile_filters
from mfs._index import SearchHit
from mfs._search_execution import SearchDeadline


class SourceProcessor(Utf8TextProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.id = "test-source-processor"
        self.media_types = (*self.media_types, "application/pdf")
        self.suffix_media_types: Mapping[str, str] = {
            **self.suffix_media_types,
            ".pdf": "application/pdf",
        }

    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
        text = staged_path.read_text()
        return ProcessedDocument(
            text, SourceMap(1, (SourceSpan(0, len(text.encode()), {"kind": "pages", "start": 7}),))
        )


def test_ranked_affixes_and_source_types_push_down_before_topk_with_no_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(
            registered.namespace, processors=[SourceProcessor()], embedder=GateEmbedder()
        )
    special = '目录/rare"\\%_Résumé.PDF'
    try:
        mfs.create_namespace(
            "n", "internal", processors=[SourceProcessor()], embedder=GateEmbedder()
        )
        mfs.create_namespace(
            "other", "internal", processors=[SourceProcessor()], embedder=GateEmbedder()
        )
        for i in range(6):
            mfs.upsert("n", f"loud-{i}.txt", b"needle needle needle")
        mfs.upsert("n", special, b"needle rare source")
        mfs.upsert("other", special, b"needle")
        mfs.wait_ready(10)
        statements: list[str] = []
        caller = threading.get_ident()

        def trace(statement: str) -> None:
            # Pooled connections also serve maintenance. Observe both the search
            # caller and its executor body, without counting unrelated cleanup.
            if threading.get_ident() == caller or threading.current_thread().name.startswith(
                "mfs-search"
            ):
                statements.append(statement)

        mfs._catalog.set_trace_callback(trace)
        # Force a candidate budget of one at the actual backend for a rare type.
        original = mfs._runtime.index("n").search

        def one_candidate(
            query: str | Sequence[float],
            *,
            mode: Literal["bm25", "vector"],
            documents: Sequence[DocumentId] | None = None,
            limit: int,
            expressions: Sequence[str] | None = None,
            deadline: SearchDeadline | None = None,
        ) -> tuple[list[SearchHit], bool]:
            return original(
                query,
                mode=mode,
                documents=documents,
                limit=1,
                expressions=expressions,
                deadline=deadline,
            )

        monkeypatch.setattr(mfs._runtime.index("n"), "search", one_candidate)
        groups: list[list[Filter]] = [
            [ByExtension("pdf")],
            [ByMediaType("application/pdf")],
            [PathPrefix('目录/rare"\\%_')],
            [PathSuffix('"\\%_Résumé.PDF')],
            [NamePrefix('rare"\\%_')],
            [NameSuffix("Résumé.PDF")],
            [ByDocumentId(DocumentId("n", special))],
        ]
        for mode in ("bm25", "vector", "hybrid"):
            for filters in groups:
                result = mfs.search("n", "needle", filters=filters, mode=mode, limit=1)
                assert result.items[0].value.document_id == DocumentId("n", special)
                assert result.items[0].value.source_location.sources == (
                    {"kind": "pages", "start": 7},
                )
                assert result.items[0].value.snapshot_id
        assert statements == []
        with pytest.raises(InvalidFilter):
            mfs.search("n", "needle", [TextMatch("needle")], mode="bm25")
        with pytest.raises(InvalidQuery):
            mfs.search("n", "needle", mode="bm25", select="doc")
    finally:
        mfs.close()


def test_under_path_excludes_prefix_siblings_and_sql_point_lookup_uses_primary_key(
    tmp_path: Path,
) -> None:
    root = tmp_path / "files"
    (root / "scope").mkdir(parents=True)
    (root / "scope-extra").mkdir()
    (root / "scope/a.txt").write_text("needle")
    (root / "scope-extra/a.txt").write_text("needle needle")
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "external", root, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("n"), 10)
        assert (
            mfs.search("n", "needle", [UnderPath("n", "scope")], mode="bm25", limit=1)
            .items[0]
            .value.document_id.doc_id
            == "scope/a.txt"
        )
        scopes = AnyOf([UnderPath("n", "scope"), UnderPath("n", "scope-extra")])
        assert len(mfs.search("n", "needle", [scopes], mode="bm25").items) == 2
        assert len(mfs.grep("n", [scopes]).items) == 2
        # Additional filters always intersect the union; they cannot expand authorized scope.
        assert (
            len(mfs.search("n", "needle", [scopes, UnderPath("n", "scope")], mode="bm25").items)
            == 1
        )
        identity = DocumentId("n", "scope/a.txt")
        compiled = compile_filters([ByDocumentId(identity)], "n", "external", search=False)
        plan = mfs._catalog.query(
            "EXPLAIN QUERY PLAN SELECT value FROM documents WHERE " + compiled.sql,
            compiled.params,
        )
        assert any("SEARCH documents USING INDEX" in str(row[3]) for row in plan)
        assert mfs.grep("n", [ByDocumentId(identity)]).items[0].value == identity
        # Multiple ID filters intersect rather than expanding a Cartesian product of batches.
        ids = [DocumentId("n", f"missing-{i}.txt") for i in range(1200)]
        assert (
            mfs.search(
                "n", "needle", [ByDocumentId([identity, *ids]), ByDocumentId(identity)], mode="bm25"
            )
            .items[0]
            .value.document_id
            == identity
        )
        assert not mfs.search(
            "n", "needle", [ByDocumentId(identity), ByDocumentId(ids)], mode="bm25"
        ).items
    finally:
        mfs.close()


def test_grep_unicode_word_smart_case_cross_chunk_and_empty_regex_validation(
    tmp_path: Path,
) -> None:
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(
            registered.namespace, processors=[Utf8TextProcessor()], chunker=ByteChunker()
        )
    try:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], chunker=ByteChunker()
        )
        with pytest.raises(InvalidPattern):
            mfs.grep("n", [TextMatch("[", regex=True)])
        # A failing Chunker must not prevent document-level grep of Unicode text.
        mfs.upsert("n", "a.txt", "café caféine CAFÉ _café café2\n中文 中文字\ncross\nline".encode())
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs._tasks.targets[DocumentId("n", "a.txt")]["stage"] != "process", 5
            )
        matches = (
            mfs.grep("n", [TextMatch("café", smart_case=True, whole_word=True)]).items[0].matches
        )
        assert len(matches) == 2
        assert (
            len(
                mfs.grep("n", [TextMatch("CAFÉ", smart_case=True, whole_word=True)])
                .items[0]
                .matches
            )
            == 1
        )
        assert len(mfs.grep("n", [TextMatch("中文", whole_word=True)]).items[0].matches) == 1
        assert mfs.grep("n", [TextMatch("cross\nline")]).items
        assert mfs.grep("n", [TextMatch("cross\\s+line", regex=True)], limit=1).items
    finally:
        mfs.close()
