# pyright: reportPrivateUsage=false
from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import Any, Literal

import pytest
from test_lifecycle import GateEmbedder

from mfs import MFS, DocumentId, GCPolicy, LocalAdmission, Utf8TextProcessor, WaitTimeout
from mfs._core import _Staged


def collect_until_deleted(mfs: MFS, path: Path) -> None:
    deadline = time.monotonic() + 5
    while path.exists():
        assert mfs.collect_garbage().error is None
        assert time.monotonic() < deadline
        time.sleep(0.01)


def test_cached_file_io_does_not_block_unrelated_status_or_cancel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    original = Path.read_bytes

    def read_bytes(path: Path) -> bytes:
        if "-processed-" in path.name and threading.current_thread().name.startswith("mfs-worker"):
            entered.set()
            assert release.wait(10)
        return original(path)

    with closing(MFS.open(tmp_path / "state", gc_policy=GCPolicy(enabled=False))) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        mfs.create_namespace(
            "other", "internal", processors=[Utf8TextProcessor()], processing_paused=True
        )
        pending = mfs.upsert("other", "pending.txt", b"pending")
        mfs.wait(mfs.upsert("n", "a.txt", b"alpha"), 10)
        monkeypatch.setattr(Path, "read_bytes", read_bytes)

        def cancel_and_read_status() -> None:
            mfs.cancel(pending.id)
            assert mfs.scope_status("other").states == {"cancelled": 1}

        with ThreadPoolExecutor(max_workers=2) as callers:
            try:
                accepted = callers.submit(mfs.upsert, "n", "b.txt", b"alpha")
                assert entered.wait(5)
                callers.submit(cancel_and_read_status).result(1)
            finally:
                release.set()
            report = accepted.result(5)
        mfs.wait(report, 10)
        document = mfs.read(report.id)
        assert document is not None and document.text == "alpha"


@pytest.mark.parametrize("race", ["replace", "corrupt_replace", "collect_payload"])
def test_cache_rechecks_mapping_and_payload_after_unlocked_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, race: str
) -> None:
    policy = GCPolicy(enabled=False, grace_seconds=0, idle_seconds=0)
    entered, release = threading.Event(), threading.Event()
    with closing(MFS.open(tmp_path / "state", gc_policy=policy)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        store = mfs._artifacts
        incarnation = mfs._tasks.namespaces["n"]["incarnation"]
        with store.operation():
            payload = store.write("payload", {}, incarnation)
            metadata = store.write("metadata", {"artifacts": {"data": payload}}, incarnation)
            store.cache("test", metadata)
        original = Path.read_bytes

        def read_bytes(path: Path) -> bytes:
            data = original(path)
            if path == store.root / metadata:
                entered.set()
                assert release.wait(10)
                if race == "corrupt_replace":
                    return b"corrupt"
            return data

        monkeypatch.setattr(Path, "read_bytes", read_bytes)

        def read_cached() -> Any:
            with store.operation():
                return store.cached("test")

        with ThreadPoolExecutor() as callers:
            future = callers.submit(read_cached)
            try:
                assert entered.wait(5)
                if race == "collect_payload":
                    collect_until_deleted(mfs, store.root / payload)
                    assert (store.root / metadata).exists()  # The metadata itself stays pinned.
                    assert not (store.root / payload).exists()
                else:
                    replacement = store.write("replacement", {"replacement": True}, incarnation)
                    store.cache("test", replacement)
            finally:
                release.set()
            assert future.result(5) is None
        if race != "collect_payload":
            assert read_cached() == {"replacement": True}  # Old read failure cannot evict it.
        assert not store._pins


def test_cache_payload_stays_pinned_until_processing_operation_finishes(tmp_path: Path) -> None:
    policy = GCPolicy(enabled=False, grace_seconds=0, idle_seconds=0)
    with closing(MFS.open(tmp_path / "state", gc_policy=policy)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        store = mfs._artifacts
        incarnation = mfs._tasks.namespaces["n"]["incarnation"]
        with store.operation():
            payload = store.write("payload", {}, incarnation)
            value = {"artifacts": {"data": payload}}
            metadata = store.write("metadata", value, incarnation)
            store.cache("test", metadata)
        with store.operation():
            assert store.cached("test") == value
            collect_until_deleted(mfs, store.root / metadata)
            assert (store.root / payload).exists()
        collect_until_deleted(mfs, store.root / payload)
        assert not store._pins


@pytest.mark.parametrize("mode", ["bm25", "hybrid"])
def test_paused_off_to_indexed_waits_for_resume_and_off_cleanup_can_finish(
    tmp_path: Path, mode: Literal["bm25", "hybrid"]
) -> None:
    model = GateEmbedder()
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        identity = mfs.upsert("n", "a.txt", b"alpha").id
        mfs.wait(identity, 10)
        mfs.configure_index("n", paused=True)
        report = mfs.configure_namespace("n", embedder=model, indexing=mode)
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: (
                    identity in mfs._tasks.build_targets
                    and mfs._tasks.build_targets[identity]["stage"] == "chunk"
                ),
                5,
            )
        with pytest.raises(WaitTimeout):
            mfs.wait(report, 0.2)
        assert mfs.namespace_configuration("n").indexing == "off"
        assert not model.calls
        assert mfs.read(identity) is not None
        mfs.configure_index("n", paused=False)
        mfs.wait(report, 10)
        assert mfs.search("n", "alpha", mode="bm25").items
        assert bool(model.calls) == (mode == "hybrid")
        mfs.configure_index("n", paused=True)
        mfs.configure_index("n", indexing="off")
        mfs.wait("n", 10)
        assert mfs.namespace_configuration("n").indexing == "off"
        assert mfs.read(identity) is not None


@pytest.mark.parametrize("during_hash", [False, True])
def test_close_interrupts_sync_without_removing_unobserved_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, during_hash: bool
) -> None:
    root = tmp_path / "source"
    root.mkdir()
    for number in range(12):
        (root / f"{number:02}.txt").write_text(f"file {number}")
    entered, release = threading.Event(), threading.Event()
    state = tmp_path / "state"
    mfs = MFS.open(state, gc_policy=GCPolicy(enabled=False))
    mfs.create_namespace("n", "external", root, processors=[Utf8TextProcessor()], indexing="off")
    mfs.wait(mfs.sync("n"), 10)
    # An incomplete rescan must preserve both unvisited existing files and this
    # previously known missing file until a later complete observation.
    (root / "11.txt").unlink()
    (root / "00.txt").write_bytes(b"x" * (3 * 1024 * 1024))
    original_stage, original_read = mfs._stage_descriptor, os.read
    descriptors: set[int] = set()
    calls: list[str] = []
    blocks: list[int] = []

    def stage(descriptor: int, source: Path) -> _Staged:
        calls.append(source.name)
        descriptors.add(descriptor)
        if not during_hash:
            entered.set()
            assert release.wait(10)
        return original_stage(descriptor, source)

    def read(descriptor: int, count: int) -> bytes:
        data = original_read(descriptor, count)
        if descriptor in descriptors and count == 1024 * 1024:
            blocks.append(len(data))
            entered.set()
            assert release.wait(10)
        return data

    monkeypatch.setattr(mfs, "_stage_descriptor", stage)
    monkeypatch.setattr(os, "read", read)
    try:
        with ThreadPoolExecutor() as callers:
            scan = callers.submit(mfs.sync, "n", verify="content")
            try:
                assert entered.wait(5)
                with pytest.raises(WaitTimeout):
                    mfs.close(timeout=0.05)
            finally:
                release.set()
            report = scan.result(5)
            assert not report.complete and not report.removed and not report.changed
            assert any(failure.code == "Closed" for failure in report.failed)
        assert calls == ["00.txt"]
        assert blocks == ([1024 * 1024] if during_hash else [])
    finally:
        release.set()
        mfs.close(timeout=10)
    with closing(MFS.open(state)) as reopened:
        reopened.open_namespace("n", processors=[Utf8TextProcessor()])
        assert len(reopened.list_document_statuses("n")) == 12
        complete = reopened.sync("n")
        assert complete.complete and complete.removed == (DocumentId("n", "11.txt"),)
        reopened.wait(complete, 10)


def test_published_query_and_other_document_embed_while_shared_model_is_busy(
    tmp_path: Path,
) -> None:
    model = GateEmbedder()  # No concurrency/resource declarations are needed.
    admission = LocalAdmission({"heavy": 1, "light": 2})
    with closing(MFS.open(tmp_path / "state", admission=admission)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=model)
        mfs.wait(mfs.upsert("n", "a.txt", b"already published alpha"), 10)
        model.entered.clear()
        model.release.clear()
        # Unrelated heavy processing must not prevent any embedding call either.
        heavy = admission.try_acquire({"heavy": 1})
        assert heavy is not None
        try:
            first = mfs.upsert("n", "b.txt", b"background beta")
            assert model.entered.wait(5)
            model.entered.clear()
            second = mfs.upsert("n", "c.txt", b"background gamma")
            assert model.entered.wait(5)
            hits = mfs.search("n", "alpha", mode="vector", select="doc_id").items
            assert [hit.value for hit in hits] == [DocumentId("n", "a.txt")]
            assert len(mfs.grep("n").items) == 3
        finally:
            heavy.release()
            model.release.set()
        mfs.wait(first, 10)
        mfs.wait(second, 10)
