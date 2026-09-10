# pyright: reportPrivateUsage=false
from __future__ import annotations

import threading
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from mfs import (
    MFS,
    ByDocumentId,
    ChunkRange,
    Closed,
    DocumentId,
    IdempotencyConflict,
    ProcessedDocument,
    SourceMap,
    StorageFailed,
    TextMatch,
    Utf8TextProcessor,
    WaitTimeout,
)
from mfs._json import JSONValue


class CountingProcessor(Utf8TextProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
        self.calls += 1
        return super().process(staged_path, media_type)


class GateEmbedder:
    embedding_space = "tests/gate/v1"
    dimension = 2

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.fail = False
        self.calls: list[tuple[str, ...]] = []

    def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        self.calls.append(tuple(texts))
        self.entered.set()
        if not self.release.wait(10):
            raise TimeoutError("test gate was not released")
        if self.fail:
            raise ValueError("injected dense failure")
        return [[1.0, 0.5] for _ in texts]

    def embed_query(self, text: str) -> Sequence[float]:
        return [1.0, 0.5]


def wait_state(mfs: MFS, identity: DocumentId, state: str) -> None:
    with mfs._condition:
        assert mfs._condition.wait_for(lambda: mfs._targets[identity]["state"] == state, 10), (
            mfs.document_status(identity)
        )


def test_dense_wait_does_not_block_grep_admission_or_eventual_search(tmp_path: Path) -> None:
    embedder = GateEmbedder()
    mfs = MFS.open(tmp_path / "state", processors=[Utf8TextProcessor()], embedder=embedder)
    try:
        mfs.create_namespace("n", "internal")
        mfs.upsert("n", "a.txt", b"old content")
        mfs.wait_ready(10)
        embedder.entered.clear()
        embedder.release.clear()
        receipt = mfs.upsert("n", "a.txt", b"new content")
        assert not receipt.index_ready
        assert embedder.entered.wait(5)
        mfs.upsert("n", "b.txt", b"second document")
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: mfs._targets[DocumentId("n", "b.txt")]["stage"] != "process", 5
            )
        assert len(mfs.query([TextMatch("new|second", regex=True)]).items) == 2
        with pytest.raises(WaitTimeout):
            mfs.search("new", mode="bm25", timeout=0)
        # Eventual vector/hybrid must not wait for the document embedding call either.
        with ThreadPoolExecutor() as pool:
            future = pool.submit(mfs.search, "old", mode="hybrid", consistency="eventual")
            try:
                assert future.result(3).items[0].value.text == "old content"
            finally:
                embedder.release.set()
        mfs.wait_ready(10)
        assert not mfs.search("old", mode="bm25").items
        assert mfs.status().ready
    finally:
        embedder.release.set()
        mfs.close()


def test_late_revision_does_not_clear_new_pending_and_cancel_can_retry(tmp_path: Path) -> None:
    embedder = GateEmbedder()
    embedder.release.clear()
    mfs = MFS.open(tmp_path / "state", processors=[Utf8TextProcessor()], embedder=embedder)
    identity = DocumentId("n", "a.txt")
    try:
        mfs.create_namespace("n", "internal")
        old = mfs.upsert("n", "a.txt", b"old")
        assert embedder.entered.wait(5)
        new = mfs.upsert("n", "a.txt", b"new")
        assert old.revision != new.revision
        mfs.cancel(identity)
        mfs.retry(identity)
        assert not mfs.status().ready
        embedder.release.set()
        mfs.wait_ready(10)
        status = mfs.document_status(identity)
        assert status is not None
        assert status.revision == status.indexed_revision == status.text_revision == new.revision
        assert mfs.search("new", mode="bm25").items
        assert not mfs.search("old", mode="bm25").items
    finally:
        embedder.release.set()
        mfs.close()


def test_cancelled_inflight_attempt_cannot_complete_after_retry(tmp_path: Path) -> None:
    processor = CountingProcessor()
    embedder = GateEmbedder()
    embedder.release.clear()
    mfs = MFS.open(tmp_path / "state", processors=[processor], embedder=embedder)
    identity = DocumentId("n", "a.txt")
    try:
        mfs.create_namespace("n", "internal")
        mfs.upsert("n", "a.txt", b"same revision")
        assert embedder.entered.wait(5)
        mfs.cancel(identity)
        mfs.retry(identity)
        embedder.release.set()
        mfs.wait_ready(10)
        assert processor.calls == 1
        assert len(embedder.calls) == 2  # The cancelled attempt must not advance the retry.
        assert mfs._index.count_document(identity) == 1
    finally:
        embedder.release.set()
        mfs.close()


class ByteChunker:
    id = "test-bytes"
    version = "1"

    def __init__(self) -> None:
        self.options: JSONValue = {}

    def chunk(self, text: str, source_map: SourceMap) -> Sequence[ChunkRange]:
        return [ChunkRange(i, i + 1) for i in range(len(text.encode()))]


def test_completed_embedding_batches_survive_reopen_without_processor(tmp_path: Path) -> None:
    class BatchEmbedder(GateEmbedder):
        def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
            if len(texts) < 128:
                self.fail = True
            return super().embed_documents(texts)

    path = tmp_path / "state"
    processor = CountingProcessor()
    failing = BatchEmbedder()
    mfs = MFS.open(path, processors=[processor], chunker=ByteChunker(), embedder=failing)
    identity = DocumentId("n", "a.txt")
    try:
        mfs.create_namespace("n", "internal")
        mfs.upsert("n", "a.txt", b"x" * 130)
        wait_state(mfs, identity, "failed")
        status = mfs.document_status(identity)
        assert status is not None and status.completed_batches == 1 and status.total_batches == 2
        assert not mfs.search("x", mode="bm25", consistency="eventual").items
    finally:
        mfs.close()
    succeeding = GateEmbedder()
    reopened = MFS.open(path, chunker=ByteChunker(), embedder=succeeding)
    try:
        reopened.retry(identity)
        reopened.wait_ready(10)
        assert processor.calls == 1
        assert [len(batch) for batch in succeeding.calls] == [2]
        assert reopened._index.count_document(identity) == 130
    finally:
        reopened.close()


def test_processing_commit_failure_reuses_completed_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    processor = CountingProcessor()
    mfs = MFS.open(tmp_path / "state", processors=[processor])
    try:
        mfs.create_namespace("n", "internal")
        original = mfs._catalog.put_document
        attempts = 0

        def fail_once(ns: str, doc: str, record: dict[str, Any]) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise StorageFailed("injected SQLite failure before text commit")
            original(ns, doc, record)

        monkeypatch.setattr(mfs._catalog, "put_document", fail_once)
        mfs.upsert("n", "a.txt", b"durable OCR output")
        mfs.wait_ready(10)
        assert processor.calls == 1 and attempts == 2
        assert mfs.query(select="doc").items[0].value.text == "durable OCR output"
    finally:
        mfs.close()


def test_idempotency_receipt_replays_after_later_update_and_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state"
    mfs = MFS.open(path, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal")
        original = mfs.upsert("n", "a.txt", b"first", idempotency_key="request-1")
        later = mfs.upsert("n", "a.txt", b"second")
        mfs.wait_ready(10)
    finally:
        mfs.close()
    reopened = MFS.open(path, processors=[Utf8TextProcessor()])
    try:
        assert reopened.upsert("n", "a.txt", b"first", idempotency_key="request-1") == original
        status = reopened.document_status(DocumentId("n", "a.txt"))
        assert status is not None and status.revision == later.revision
        with pytest.raises(IdempotencyConflict):
            reopened.upsert("n", "a.txt", b"different", idempotency_key="request-1")
        assert (
            reopened.query([ByDocumentId(DocumentId("n", "a.txt"))], select="doc")
            .items[0]
            .value.text
            == "second"
        )
    finally:
        reopened.close()


def test_close_wakes_strong_waiter_and_joins_workers(tmp_path: Path) -> None:
    embedder = GateEmbedder()
    embedder.fail = True
    mfs = MFS.open(tmp_path / "state", processors=[Utf8TextProcessor()], embedder=embedder)
    try:
        mfs.create_namespace("n", "internal")
        mfs.upsert("n", "a.txt", b"text")
        wait_state(mfs, DocumentId("n", "a.txt"), "failed")
        entered = threading.Event()

        def waiting() -> None:
            entered.set()
            mfs.wait_ready()

        with ThreadPoolExecutor() as pool:
            future = pool.submit(waiting)
            assert entered.wait(5)
            mfs.close()
            with pytest.raises(Closed):
                future.result(5)
        assert all(not thread.is_alive() for thread in mfs._workers)
    finally:
        mfs.close()
