# pyright: reportPrivateUsage=false
from __future__ import annotations

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

import pytest
from test_concurrent_lifecycle import Model

from mfs import (
    MFS,
    Closed,
    DocumentId,
    ExecutionPolicy,
    GCPolicy,
    InstanceLocked,
    OperationFailed,
    ProcessedDocument,
    ProcessingContext,
    UnderPath,
    Utf8TextProcessor,
    WaitTimeout,
)
from mfs._catalog import Catalog


def test_slow_root_revalidation_does_not_block_other_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "source"
    root.mkdir()
    entered, release = threading.Event(), threading.Event()
    original = Path.resolve
    calls = 0

    def resolve(path: Path, strict: bool = False) -> Path:
        nonlocal calls
        if path == root and threading.current_thread().name.startswith("scanner"):
            calls += 1
            if calls == 2:
                entered.set()
                assert release.wait(5)
        return original(path, strict=strict)

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("external", "external", root=root, processors=[Utf8TextProcessor()])
        mfs.create_namespace("other", "internal", processors=[Utf8TextProcessor()])
        monkeypatch.setattr(Path, "resolve", resolve)
        with (
            ThreadPoolExecutor(thread_name_prefix="scanner") as scanner,
            ThreadPoolExecutor() as reader,
        ):
            try:
                scan = scanner.submit(mfs.sync, "external")
                assert entered.wait(3)
                reader.submit(mfs.scope_status, "other").result(0.5)
                lease = reader.submit(mfs.quiesce, [UnderPath("other")], timeout=0.5).result(1)
                lease.close()
            finally:
                release.set()
            assert scan.result(5).complete


@pytest.mark.parametrize("reenable", [False, True])
def test_index_toggles_follow_latest_intent(tmp_path: Path, reenable: bool) -> None:
    model = Model("candidate")
    model.release.clear()
    with closing(MFS.open(tmp_path / "state", gc_policy=GCPolicy(enabled=False))) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        mfs.wait(mfs.upsert("n", "a.txt", b"alpha"), 10)
        try:
            mfs.configure_namespace("n", embedder=model, indexing="hybrid")
            assert model.entered.wait(5)
            mfs.configure_index("n", indexing="off")
            if reenable:
                mfs.configure_index("n", indexing="hybrid")
        finally:
            model.release.set()
        mfs.wait("n", 10)
        assert mfs.namespace_configuration("n").indexing == ("hybrid" if reenable else "off")
        assert bool(mfs.search("n", "alpha", mode="vector").items) == reenable
        assert mfs.grep("n").items  # Disabling ranking retains the accepted source text.


def test_short_lived_callers_reuse_catalog_connections(tmp_path: Path) -> None:
    with closing(MFS.open(tmp_path / "state", gc_policy=GCPolicy(enabled=False))) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        for _ in range(100):
            with ThreadPoolExecutor(max_workers=1) as caller:
                caller.submit(mfs.scope_status, "n").result(2)
        assert len(mfs._catalog._connections) <= 8


def test_superseded_candidate_stops_at_checkpoint(tmp_path: Path) -> None:
    entered, release = threading.Event(), threading.Event()
    continued = threading.Event()

    class Processor(Utf8TextProcessor):
        def __init__(self) -> None:
            super().__init__()
            self.version = "candidate"

        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext | None = None
        ) -> ProcessedDocument:
            entered.set()
            assert release.wait(5)
            assert context is not None
            context.checkpoint({"page": 1})
            continued.set()
            return super().process(staged_path, media_type)

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        mfs.wait(mfs.upsert("n", "a.txt", b"text"), 10)
        try:
            mfs.configure_namespace("n", processors=[Processor()])
            assert entered.wait(5)
            latest = mfs.configure_namespace("n", processors=[Utf8TextProcessor()])
        finally:
            release.set()
        mfs.wait(latest, 10)
        assert not continued.is_set()


def test_stage_timeout_keeps_actual_lease_and_close_is_bounded(tmp_path: Path) -> None:
    model = Model("timeout")
    model.release.clear()
    state = tmp_path / "state"
    mfs = MFS.open(state, execution=ExecutionPolicy(stage_timeout=0.3))
    identity = DocumentId("n", "a.txt")
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=model)
        mfs.upsert("n", "a.txt", b"alpha")
        assert model.entered.wait(5)
        with pytest.raises(OperationFailed) as failure:
            mfs.wait(identity, 3)
        assert failure.value.error_code == "ExecutionTimeout"
        current = mfs.document_status(identity)
        assert current is not None and current.executing
        mfs.retry(identity)
        with pytest.raises(WaitTimeout):
            mfs.wait(identity, 0.05)
        started = time.monotonic()
        with pytest.raises(WaitTimeout):
            mfs.close(timeout=0.05)
        assert time.monotonic() - started < 1
        with pytest.raises(Closed):
            mfs.status()
        with pytest.raises(InstanceLocked):
            MFS.open(state)
    finally:
        model.release.set()
        mfs.close(timeout=10)
    with closing(MFS.open(state)) as recovered:
        recovered.open_namespace("n", processors=[Utf8TextProcessor()], embedder=model)
        recovered.wait(identity, 10)
        assert recovered.search("n", "alpha", mode="vector").items


def test_timed_out_close_does_not_wait_for_lifecycle_lock(tmp_path: Path) -> None:
    mfs = MFS.open(tmp_path / "state")
    entered, release = threading.Event(), threading.Event()

    def hold_lock() -> None:
        with mfs._condition:
            entered.set()
            assert release.wait(5)

    with ThreadPoolExecutor() as pool:
        try:
            held = pool.submit(hold_lock)
            assert entered.wait(3)
            started = time.monotonic()
            with pytest.raises(WaitTimeout):
                mfs.close(timeout=0.05)
            assert time.monotonic() - started < 1
        finally:
            release.set()
            mfs.close(timeout=10)
        held.result(1)


def test_catalog_pool_isolates_concurrent_transactions_and_rollbacks(tmp_path: Path) -> None:
    with closing(Catalog(tmp_path / "catalog.sqlite", initialize=True)) as catalog:
        catalog.execute("CREATE TABLE counter (value INTEGER)")
        catalog.execute("INSERT INTO counter VALUES (0)")

        def increment(number: int) -> None:
            try:
                with catalog.transaction():
                    value = catalog.query("SELECT value FROM counter")[0][0]
                    with catalog.transaction():
                        catalog.execute("UPDATE counter SET value=?", (value + 1,))
                    if number % 2:
                        raise ValueError("rollback")
            except ValueError:
                pass

        with ThreadPoolExecutor(max_workers=24) as callers:
            list(callers.map(increment, range(100)))
        assert catalog.query("SELECT value FROM counter") == [(50,)]
        assert len(catalog._connections) <= 8


def test_pending_off_and_pause_survive_reopen(tmp_path: Path) -> None:
    state = tmp_path / "state"
    with closing(MFS.open(state, start_paused=True)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
        mfs.upsert("n", "a.txt", b"text")
        mfs.configure_namespace("n", embedder=Model("unused"), indexing="hybrid")
        mfs.configure_index("n", paused=True)
        assert mfs._tasks.namespaces["n"]["building"]["indexing"] == "hybrid"
        mfs.configure_index("n", indexing="off")
        revision = mfs.namespace_configuration("n").pending_revision
    with closing(MFS.open(state)) as recovered:
        recovered.open_namespace("n", processors=[Utf8TextProcessor()])
        recovered.open_namespace(
            "n", processors=[Utf8TextProcessor()], configuration_revision=revision
        )
        recovered.configure_index("n", paused=False)
        recovered.wait("n", 10)
        assert recovered.namespace_configuration("n").indexing == "off"
        assert recovered.grep("n").items


def test_stage_deadline_retires_managed_process_and_preserves_failure(tmp_path: Path) -> None:
    exited = threading.Event()

    class Processor(Utf8TextProcessor):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext | None = None
        ) -> ProcessedDocument:
            assert context is not None
            try:
                assert context.cancellation.remaining() is not None
                context.run_process([sys.executable, "-c", "import time; time.sleep(60)"])
            finally:
                exited.set()
            return super().process(staged_path, media_type)

    state = tmp_path / "state"
    with closing(MFS.open(state, execution=ExecutionPolicy(stage_timeout=0.5))) as mfs:
        mfs.create_namespace("n", "internal", processors=[Processor()], indexing="off")
        report = mfs.upsert("n", "a.txt", b"text")
        with pytest.raises(OperationFailed) as failure:
            mfs.wait(report, 5)
        assert failure.value.error_code == "ExecutionTimeout"
        assert exited.wait(3)
    with closing(MFS.open(state)) as recovered:
        with pytest.raises(OperationFailed) as failure:
            recovered.wait(report, 0)
        assert failure.value.error_code == "ExecutionTimeout"
