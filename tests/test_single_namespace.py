from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import pytest

import mfs as public
from mfs import (
    MFS,
    AnyOf,
    ByDocumentId,
    DocumentId,
    InvalidFilter,
    InvalidNamespace,
    NamespaceNotFound,
    UnderPath,
    Utf8TextProcessor,
)


class CountingModel:
    embedding_space = "single-namespace-tests"
    dimension = 2

    def __init__(self) -> None:
        self.queries = 0

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        return [[1.0, 0.0] for _ in texts]

    def embed_query(self, text: str) -> list[float]:
        self.queries += 1
        return [1.0, 0.0]


def test_queries_require_one_namespace_and_never_route_or_wait_for_others(tmp_path: Path) -> None:
    model = CountingModel()
    mfs = MFS.open(tmp_path / "state")
    try:
        for namespace in ("a", "b"):
            mfs.create_namespace(
                namespace, "internal", processors=[Utf8TextProcessor()], embedder=model
            )
            mfs.wait(mfs.upsert(namespace, "same.txt", b"needle"), 10)
        mfs.configure_index("b", paused=True)
        mfs.upsert("b", "pending.txt", b"needle still pending")
        result = mfs.search("a", "needle", consistency="strong", timeout=2)
        assert [item.value.document_id for item in result.items] == [DocumentId("a", "same.txt")]
        assert model.queries == 1
        with pytest.raises(InvalidFilter):
            mfs.search("b", "needle", filters=[UnderPath("a")], consistency="strong", timeout=0.1)
        assert [item.value for item in mfs.grep("a").items] == [DocumentId("a", "same.txt")]
        assert not hasattr(public, "ByNamespace")
        with pytest.raises(TypeError):
            cast(Any, mfs.search)("needle")
        with pytest.raises(TypeError):
            cast(Any, mfs.grep)()
        for invalid in (None, ["a", "b"], ("a", "b"), ""):
            with pytest.raises(InvalidNamespace):
                mfs.search(cast(Any, invalid), "needle")
            with pytest.raises(InvalidNamespace):
                mfs.grep(cast(Any, invalid))
        with pytest.raises(NamespaceNotFound):
            mfs.search("missing", "needle")
        with pytest.raises(NamespaceNotFound):
            mfs.grep("missing")
    finally:
        mfs.close()


def test_filters_cannot_escape_namespace_even_inside_anyof(tmp_path: Path) -> None:
    mfs = MFS.open(tmp_path / "state")
    try:
        for namespace in ("a", "b"):
            root = tmp_path / namespace
            root.mkdir()
            (root / "same.txt").write_text("needle")
            mfs.create_namespace(namespace, "external", root, processors=[Utf8TextProcessor()])
            mfs.wait(mfs.sync(namespace), 10)
        foreign = ByDocumentId(DocumentId("b", "same.txt"))
        filters = (
            foreign,
            ByDocumentId([DocumentId("a", "same.txt"), DocumentId("b", "same.txt")]),
            UnderPath("b"),
            AnyOf([UnderPath("a"), AnyOf([foreign])]),
        )
        for item in filters:
            with pytest.raises(InvalidFilter, match="namespace"):
                mfs.search("a", "needle", filters=[item], mode="bm25")
            with pytest.raises(InvalidFilter, match="namespace"):
                mfs.grep("a", filters=[item])
    finally:
        mfs.close()
