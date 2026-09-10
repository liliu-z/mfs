# pyright: reportPrivateUsage=false
from __future__ import annotations

import random
from pathlib import Path

import pytest

from mfs import DocumentId
from mfs._index import ChunkIndex, IndexRow


@pytest.mark.conformance
def test_multisegment_scan_over_16384_rows_is_complete_and_reopenable(tmp_path: Path) -> None:
    index_path = tmp_path / "milvus.db"
    index = ChunkIndex(index_path)
    index.recreate(dense_dimension=None)
    ordinals = list(range(16_500))
    random.Random(7).shuffle(ordinals)
    expected = {("ns", f"doc-{ordinal:05d}", 0) for ordinal in ordinals}
    try:
        for start in range(0, len(ordinals), 5_500):
            rows = [
                IndexRow(
                    namespace="ns",
                    doc_id=f"doc-{ordinal:05d}",
                    ordinal=0,
                    text=f"conformance token {ordinal}",
                    text_start=0,
                    text_end=len(f"conformance token {ordinal}"),
                    dense_vector=[],
                )
                for ordinal in ordinals[start : start + 5_500]
            ]
            index.insert(rows)
            index.flush()
        scanned = index.scan()
        actual: set[tuple[str, str, int]] = set()
        for row in scanned:
            ordinal = row["ordinal"]
            assert isinstance(ordinal, int)
            actual.add((str(row["namespace"]), str(row["doc_id"]), ordinal))
        assert actual == expected
        hits, more = index.search(
            "conformance",
            mode="bm25",
            documents=None,
            limit=1000,
        )
        assert len(hits) == 1000
        assert more
        index.delete_document(DocumentId("ns", "doc-00000"))
        index.flush()
    finally:
        index.close()

    reopened = ChunkIndex(index_path)
    try:
        assert reopened.has_valid_collection(dense_dimension=None)
        reopened.load()
        assert len(reopened.scan()) == 16_499
    finally:
        reopened.close()
