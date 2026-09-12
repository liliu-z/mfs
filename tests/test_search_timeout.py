# pyright: reportPrivateUsage=false
from __future__ import annotations

import threading
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal

import pytest
from test_lifecycle import GateEmbedder

from mfs import MFS, DocumentId, Utf8TextProcessor, WaitTimeout
from mfs._index import SearchHit
from mfs._search_execution import SearchDeadline


class SlowQuery(GateEmbedder):
    def __init__(self) -> None:
        super().__init__()
        self.query_condition = threading.Condition()
        self.query_calls = 0
        self.query_release = threading.Event()

    def embed_query(self, text: str) -> Sequence[float]:
        with self.query_condition:
            self.query_calls += 1
            self.query_condition.notify_all()
        assert self.query_release.wait(10)
        return super().embed_query(text)

    def wait_queries(self, count: int) -> None:
        with self.query_condition:
            assert self.query_condition.wait_for(lambda: self.query_calls >= count, 3)


def test_caller_times_out_during_query_embedding_and_close_retains_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    embedder = SlowQuery()
    mfs = MFS.open(tmp_path / "state")
    backend_called = threading.Event()

    def unexpected_backend(*args: object, **kwargs: object) -> None:
        backend_called.set()
        raise AssertionError("expired query reached the backend")

    with ThreadPoolExecutor() as pool:
        try:
            mfs.create_namespace(
                "n", "internal", processors=[Utf8TextProcessor()], embedder=embedder
            )
            mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
            monkeypatch.setattr(mfs._runtime.index("n"), "search", unexpected_backend)
            future = pool.submit(mfs.search, "n", "needle", mode="vector", timeout=0.3)
            embedder.wait_queries(1)
            with pytest.raises(WaitTimeout):
                future.result(2)
            assert not embedder.query_release.is_set()
            closing = pool.submit(mfs.close)
            # Closing must not free resources still leased by the timed-out adapter.
            with pytest.raises(TimeoutError):
                closing.result(0.1)
            embedder.query_release.set()
            closing.result(5)
            assert not backend_called.is_set()
        finally:
            embedder.query_release.set()
            mfs.close()


def test_timeout_during_backend_does_not_start_next_hybrid_channel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mfs = MFS.open(tmp_path / "state")
    entered, release = threading.Event(), threading.Event()
    modes: list[str] = []
    with ThreadPoolExecutor() as pool:
        try:
            mfs.create_namespace(
                "n", "internal", processors=[Utf8TextProcessor()], embedder=GateEmbedder()
            )
            mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
            original = mfs._runtime.index("n").search

            def slow_backend(
                query: str | Sequence[float],
                *,
                mode: Literal["bm25", "vector"],
                documents: Sequence[DocumentId] | None = None,
                limit: int,
                expressions: Sequence[str] | None = None,
                deadline: SearchDeadline | None = None,
            ) -> tuple[list[SearchHit], bool]:
                modes.append(mode)
                entered.set()
                assert release.wait(10)
                return original(
                    query,
                    mode=mode,
                    documents=documents,
                    limit=limit,
                    expressions=expressions,
                    deadline=deadline,
                )

            monkeypatch.setattr(mfs._runtime.index("n"), "search", slow_backend)
            future = pool.submit(mfs.search, "n", "needle", mode="hybrid", timeout=0.3)
            assert entered.wait(3)
            with pytest.raises(WaitTimeout):
                future.result(2)
            assert not release.is_set()
            release.set()
            mfs.close()
            assert modes == ["bm25"]
        finally:
            release.set()
            mfs.close()


def test_timed_out_adapters_keep_concurrency_slots_and_admission_has_same_deadline(
    tmp_path: Path,
) -> None:
    embedder = SlowQuery()
    mfs = MFS.open(tmp_path / "state")
    with ThreadPoolExecutor(max_workers=4) as pool:
        try:
            mfs.create_namespace(
                "n", "internal", processors=[Utf8TextProcessor()], embedder=embedder
            )
            mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
            futures = [
                pool.submit(mfs.search, "n", "needle", mode="vector", timeout=1) for _ in range(4)
            ]
            embedder.wait_queries(4)
            for future in futures:
                with pytest.raises(WaitTimeout):
                    future.result(2)
            with pytest.raises(WaitTimeout):
                mfs.search("n", "needle", mode="vector", timeout=0.1)
            assert embedder.query_calls == 4
            embedder.query_release.set()
            assert mfs.search("n", "needle", mode="vector", timeout=None).items
            with pytest.raises(WaitTimeout):
                mfs.search("n", "needle", mode="bm25", timeout=0)
        finally:
            embedder.query_release.set()
            mfs.close()
