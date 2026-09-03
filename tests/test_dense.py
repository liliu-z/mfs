from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from mfs import MFS, CapabilityUnavailable, IndexUnavailable, Utf8TextProcessor


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
    state = tmp_path / "state"
    mfs = MFS.open(state, processors=[Utf8TextProcessor()], embedder=TinyEmbedder())
    mfs.create_namespace("n", "internal")
    mfs.upsert("n", "cat.txt", b"cat cat")
    mfs.upsert("n", "dog.txt", b"dog dog")
    assert mfs.search("cat", mode="vector").items[0].value.document_id.doc_id == "cat.txt"
    assert mfs.search("dog", mode="hybrid").items[0].value.document_id.doc_id == "dog.txt"
    mfs.close()

    without = MFS.open(state, processors=[Utf8TextProcessor()])
    try:
        status = without.status()
        assert status.dense_enabled and not status.dense_available
        assert without.search("cat", mode="bm25").items
        with pytest.raises(CapabilityUnavailable):
            without.search("cat", mode="vector")
        assert without.upsert("n", "cat.txt", b"cat cat").outcome == "unchanged"
        with pytest.raises(CapabilityUnavailable):
            without.upsert("n", "cat.txt", b"changed cat")
    finally:
        without.close()


def test_reindex_upgrades_bm25_index_to_dense(tmp_path: Path) -> None:
    state = tmp_path / "state"
    bm25 = MFS.open(state, processors=[Utf8TextProcessor()])
    bm25.create_namespace("n", "internal")
    bm25.upsert("n", "cat.txt", b"cat")
    bm25.close()

    upgrading = MFS.open(state, processors=[Utf8TextProcessor()], embedder=TinyEmbedder())
    try:
        assert upgrading.status().index_state == "mismatch"
        with pytest.raises(IndexUnavailable):
            upgrading.search("cat", mode="bm25")
        report = upgrading.reindex()
        assert report.dense_enabled
        assert upgrading.search("cat", mode="vector").items
    finally:
        upgrading.close()
