from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from mfs import MFS, CapabilityUnavailable, OperationFailed, Utf8TextProcessor


class TinyEmbedder:
    embedding_space = "tests/tiny/v1"
    dimension = 2

    def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return [self._vector(text) for text in texts]

    def embed_query(self, text: str) -> Sequence[float]:
        return self._vector(text)

    @staticmethod
    def _vector(text: str) -> list[float]:
        lowered = text.lower()
        return [float(lowered.count("cat")), float(lowered.count("dog"))]


def test_dense_and_hybrid_then_open_without_embedder(tmp_path: Path) -> None:
    from mfs import InvalidConfiguration

    state = tmp_path / "state"
    mfs = MFS.open(state)
    try:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], embedder=TinyEmbedder()
        )
        mfs.upsert("n", "cat.txt", b"cat cat")
        mfs.upsert("n", "dog.txt", b"dog dog")
        assert (
            mfs.search("n", "cat", mode="vector", consistency="strong", timeout=10)
            .items[0]
            .value.document_id.doc_id
            == "cat.txt"
        )
        assert (
            mfs.search("n", "dog", mode="hybrid", timeout=10).items[0].value.document_id.doc_id
            == "dog.txt"
        )
    finally:
        mfs.close()
    without = MFS.open(state)
    try:
        assert without.status().dense_enabled and not without.status().dense_available
        assert without.search("n", "cat", mode="bm25", timeout=10).items
        with pytest.raises(InvalidConfiguration):
            without.open_namespace("n", processors=[Utf8TextProcessor()])
        with pytest.raises(CapabilityUnavailable):
            without.search("n", "cat", mode="vector", timeout=10)
        receipt = without.upsert("n", "cat.txt", b"changed cat")
        with pytest.raises(OperationFailed) as blocked:
            without.wait(receipt, 0.1)
        assert blocked.value.state == "blocked"
        assert not without.search("n", "cat", mode="bm25", consistency="eventual").items
        without.open_namespace("n", processors=[Utf8TextProcessor()], embedder=TinyEmbedder())
        without.wait(receipt, 10)
        assert without.grep("n", select="doc").items[0].value.text == "changed cat"
    finally:
        without.close()


def test_reindex_upgrades_bm25_index_to_dense(tmp_path: Path) -> None:
    from mfs import NamespaceCompatibilityError

    state = tmp_path / "state"
    mfs = MFS.open(state)
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "cat.txt", b"cat"), 10)
    finally:
        mfs.close()
    mfs = MFS.open(state)
    try:
        with pytest.raises(NamespaceCompatibilityError):
            mfs.open_namespace("n", processors=[Utf8TextProcessor()], embedder=TinyEmbedder())
        assert mfs.search("n", "cat", mode="bm25", timeout=10).items
        report = mfs.reindex(
            "n",
            timeout=10,
            processors=[Utf8TextProcessor()],
            embedder=TinyEmbedder(),
            indexing="hybrid",
        )
        assert report.dense_enabled
        assert mfs.search("n", "cat", mode="vector", timeout=10).items
    finally:
        mfs.close()
