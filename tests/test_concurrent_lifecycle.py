# pyright: reportPrivateUsage=false
from __future__ import annotations

import threading
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import ClassVar

import blake3
import pytest

from mfs import (
    MFS,
    DocumentId,
    ExecutionPolicy,
    OperationFailed,
    ProcessedDocument,
    SourceMap,
    StorageFailed,
    Superseded,
    TextMatch,
    UnderPath,
    Utf8TextProcessor,
    WaitTimeout,
)


class ParallelText(Utf8TextProcessor):
    concurrency = 2

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Barrier(3)
        self.release = threading.Event()

    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
        self.entered.wait(5)
        assert self.release.wait(10)
        return super().process(staged_path, media_type)


def test_distinct_files_execute_together_with_durable_active_inputs(tmp_path: Path) -> None:
    processor = ParallelText()
    with closing(MFS.open(tmp_path / "state", execution=ExecutionPolicy(workers=2))) as mfs:
        mfs.create_namespace("n", "internal", processors=[processor], indexing="off")
        try:
            mfs.upsert("n", "a.txt", b"a")
            mfs.upsert("n", "b.txt", b"b")
            processor.entered.wait(5)
            with mfs._condition:
                assert len(mfs._tasks.executing) == 2
                rows = mfs._catalog.query("SELECT value FROM active_runs")
                assert len(rows) == 2
                for row in rows:
                    active = mfs._catalog.decode(row[0])
                    assert active["active_run_id"] and active["attempt_token"]
                    assert (tmp_path / "state" / active["input"]).is_file()
        finally:
            processor.release.set()
        mfs.wait("n", 10)


def test_superseded_processor_failure_does_not_fail_latest_input(tmp_path: Path) -> None:
    entered, release = threading.Event(), threading.Event()
    observed: list[str] = []

    class Processor(Utf8TextProcessor):
        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            text = staged_path.read_text()
            observed.append(text)
            if text == "v2":
                entered.set()
                assert release.wait(10)
                assert staged_path.read_text() == "v2"
                raise ValueError("old operator failed")
            return super().process(staged_path, media_type)

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Processor()])
        try:
            mfs.upsert("n", "a.txt", b"v2")
            assert entered.wait(5)
            mfs.upsert("n", "a.txt", b"v3")
            latest = mfs.upsert("n", "a.txt", b"v4")
            assert observed == ["v2"]
        finally:
            release.set()
        mfs.wait(latest, 10)
        assert observed == ["v2", "v4"]
        assert mfs.search("n", "v4", mode="bm25").items


def test_resource_admission_failure_is_reported_without_losing_workers(tmp_path: Path) -> None:
    class Invalid(Utf8TextProcessor):
        resources: ClassVar[dict[str, int]] = {"unconfigured": 1}

    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("bad", "internal", processors=[Invalid()])
        with pytest.raises(OperationFailed, match="resource"):
            mfs.wait(mfs.upsert("bad", "a.txt", b"bad"), 10)
        mfs.create_namespace("good", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("good", "a.txt", b"good"), 10)
        assert mfs.search("good", "good", mode="bm25").items


class Model:
    dimension = 2
    resources: ClassVar[dict[str, int]] = {}  # Remote model; no shared local compute.

    def __init__(self, space: str) -> None:
        self.embedding_space = space
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.fail = False

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        self.entered.set()
        assert self.release.wait(10)
        if self.fail:
            raise ValueError("model deliberately failed")
        return [[1.0, 0.0] for _ in texts]

    def embed_query(self, text: str) -> list[float]:
        return [1.0, 0.0]


def test_repeated_configuration_changes_coalesce_after_actual_exit(tmp_path: Path) -> None:
    old, middle, skipped, final = (Model(name) for name in ("old", "middle", "skip", "final"))
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=old)
        mfs.wait(mfs.upsert("n", "a.txt", b"needle"), 10)
        middle.release.clear()
        try:
            mfs.configure_namespace("n", embedder=middle)
            assert middle.entered.wait(5)
            mfs.configure_namespace("n", embedder=skipped)
            newest = mfs.configure_namespace("n", embedder=final)
            assert not final.entered.is_set()
            assert not skipped.entered.is_set()
            assert mfs.search("n", "needle", mode="hybrid").items
        finally:
            middle.release.set()
        mfs.wait(newest, 10)
        assert final.entered.is_set() and not skipped.entered.is_set()
        assert mfs.namespace_configuration("n").active_revision == newest.revision


def test_failed_candidate_keeps_active_search_and_retry_can_promote(tmp_path: Path) -> None:
    old, new = Model("old"), Model("new")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=old)
        mfs.wait(mfs.upsert("n", "a.txt", b"visible text"), 10)
        active = mfs.namespace_configuration("n").active_revision
        new.fail = True
        accepted = mfs.configure_namespace("n", embedder=new)
        with pytest.raises(OperationFailed):
            mfs.wait(accepted, 10)
        assert mfs.namespace_configuration("n").active_revision == active
        assert mfs.search("n", "visible", mode="hybrid").items
        new.fail = False
        mfs.retry(DocumentId("n", "a.txt"))
        mfs.wait(accepted, 10)
        assert mfs.namespace_configuration("n").active_revision == accepted.revision
        assert mfs.search("n", "visible", mode="hybrid").items


def test_delete_during_candidate_build_is_immediately_invisible(tmp_path: Path) -> None:
    old, new = Model("old"), Model("new")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=old)
        mfs.wait(mfs.upsert("n", "a.txt", b"delete me"), 10)
        new.release.clear()
        try:
            accepted = mfs.configure_namespace("n", embedder=new)
            assert new.entered.wait(5)
            mfs.remove("n", "a.txt")
            assert not mfs.search("n", "delete", mode="bm25").items
        finally:
            new.release.set()
        mfs.wait(accepted, 10)
        assert not mfs.search("n", "delete", mode="bm25", consistency="strong").items


def test_reopen_can_bind_active_and_candidate_independently(tmp_path: Path) -> None:
    old, new = Model("old"), Model("new")
    state = tmp_path / "state"
    with closing(MFS.open(state)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=old)
        mfs.wait(mfs.upsert("n", "a.txt", b"persistent text"), 10)
        new.fail = True
        accepted = mfs.configure_namespace("n", embedder=new)
        with pytest.raises(OperationFailed):
            mfs.wait(accepted, 10)
    with closing(MFS.open(state)) as mfs:
        mfs.open_namespace("n", processors=[Utf8TextProcessor()], embedder=old)
        assert mfs.search("n", "persistent", mode="hybrid").items
        with pytest.raises(OperationFailed):
            mfs.wait("n", 1)
        new.fail = False
        mfs.open_namespace(
            "n",
            processors=[Utf8TextProcessor()],
            embedder=new,
            configuration_revision=accepted.revision,
        )
        mfs.retry(DocumentId("n", "a.txt"))
        mfs.wait("n", 10)
        assert mfs.search("n", "persistent", mode="hybrid").items


def test_query_model_capacity_wait_respects_caller_deadline(tmp_path: Path) -> None:
    model = Model("shared")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=model)
        mfs.wait(mfs.upsert("n", "a.txt", b"existing"), 10)
        model.entered.clear()
        model.release.clear()
        try:
            mfs.upsert("n", "b.txt", b"pending")
            assert model.entered.wait(5)
            with pytest.raises(WaitTimeout):
                mfs.search("n", "existing", mode="hybrid", timeout=0.05)
        finally:
            model.release.set()
        mfs.wait("n", 10)


def test_strong_grep_uses_candidate_text_without_waiting_for_embedding(tmp_path: Path) -> None:
    class Revised(Utf8TextProcessor):
        def __init__(self) -> None:
            super().__init__()
            self.version = "2"

        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            return ProcessedDocument("new extraction", SourceMap(1, ()))

    old, new = Model("old"), Model("new")
    new.fail = True
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=old)
        mfs.wait(mfs.upsert("n", "a.txt", b"old extraction"), 10)
        accepted = mfs.configure_namespace("n", processors=[Revised()], embedder=new)
        with pytest.raises(OperationFailed):
            mfs.wait(accepted, 10)
        assert mfs.grep("n", [TextMatch("old")]).items
        assert mfs.grep("n", [TextMatch("new")], consistency="strong").items
        status = mfs.document_status(DocumentId("n", "a.txt"))
        assert status is not None and status.text_revision == status.revision
        with pytest.raises(OperationFailed):
            mfs.search("n", "new", consistency="strong")


def test_boot_gate_allows_path_recovery_before_any_execution(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    (root / "a.txt").write_text("source")
    state = tmp_path / "state"
    with closing(MFS.open(state, start_paused=True)) as mfs:
        mfs.create_namespace("n", "external", root, processors=[Utf8TextProcessor()])
        mfs.sync("n")
        assert not mfs._tasks.executing
        # Simulate a host crash after disk rename but before its acceptance ACK.
        (root / "a.txt").rename(root / "b.txt")
    with closing(MFS.open(state, start_paused=True)) as mfs:
        mfs.open_namespace("n", processors=[Utf8TextProcessor()])
        with mfs.quiesce([UnderPath("n")]):
            report = mfs.sync("n", verify="content")
            assert not report.failed
            assert not mfs._tasks.executing
        mfs.resume_background()
        mfs.wait(report, 10)
        hits = mfs.search("n", "source", mode="bm25", select="doc_id").items
        assert [hit.value for hit in hits] == [DocumentId("n", "b.txt")]


def test_all_configuration_waits_surface_fatal_storage_error(tmp_path: Path) -> None:
    with closing(MFS.open(tmp_path / "state", start_paused=True)) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        with mfs._condition:
            mfs._tasks.storage_error = StorageFailed("storage is unavailable")
            mfs._condition.notify_all()
        with pytest.raises(StorageFailed):
            mfs.reindex("n", timeout=None)


@pytest.mark.parametrize("later", ["update", "delete"])
def test_staged_internal_request_cannot_overwrite_later_intent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, later: str
) -> None:
    from mfs import GCPolicy

    with (
        closing(
            MFS.open(
                tmp_path / "state",
                gc_policy=GCPolicy(enabled=False, idle_seconds=0, grace_seconds=0),
            )
        ) as mfs,
        ThreadPoolExecutor() as pool,
    ):
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"initial"), 10)
        entered, release = threading.Event(), threading.Event()
        original = mfs._prepare_original
        pinned: list[Path] = []

        def delayed(staged: object, incarnation: str) -> None:
            from mfs._core import _Staged

            assert isinstance(staged, _Staged)
            original(staged, incarnation)
            if staged.content_hash != blake3.blake3(b"slow").hexdigest():
                return
            pinned.append(staged.path)
            entered.set()
            assert release.wait(10)

        monkeypatch.setattr(mfs, "_prepare_original", delayed)
        future = pool.submit(mfs.upsert, "n", "a.txt", b"slow")
        try:
            assert entered.wait(5)
            report = (
                mfs.upsert("n", "a.txt", b"latest")
                if later == "update"
                else mfs.remove("n", "a.txt")
            )
            for _ in range(4):
                assert mfs.collect_garbage().error is None
            assert pinned[0].read_bytes() == b"slow"
        finally:
            release.set()
        with pytest.raises(Superseded):
            future.result(10)
        mfs.wait(report, 10)
        document = mfs.read(DocumentId("n", "a.txt"))
        assert (document.original if document else None) == (
            b"latest" if later == "update" else None
        )


def test_new_format_can_be_accepted_while_configuration_is_building(tmp_path: Path) -> None:
    class NewFormat(Utf8TextProcessor):
        def __init__(self) -> None:
            super().__init__()
            self.id = "new-format"
            self.media_types = ("text/x-new-format",)
            self.suffix_media_types = {".new": "text/x-new-format"}

    old, new = Model("old"), Model("new")
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=old)
        mfs.wait(mfs.upsert("n", "a.txt", b"existing"), 10)
        new.release.clear()
        try:
            changed = mfs.configure_namespace(
                "n", processors=[Utf8TextProcessor(), NewFormat()], embedder=new
            )
            assert new.entered.wait(5)
            mfs.upsert("n", "b.new", b"new format needle")
        finally:
            new.release.set()
        mfs.wait(changed, 10)
        assert mfs.search("n", "needle", mode="bm25", select="doc_id").items[0].value == DocumentId(
            "n", "b.new"
        )


def test_old_query_keeps_its_publication_across_processor_cutover(tmp_path: Path) -> None:
    entered, release = threading.Event(), threading.Event()

    class OldQuery(Model):
        def embed_query(self, text: str) -> list[float]:
            entered.set()
            assert release.wait(10)
            return super().embed_query(text)

    class Revised(Utf8TextProcessor):
        def __init__(self) -> None:
            super().__init__()
            self.version = "2"

        def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
            return ProcessedDocument("new text", SourceMap(1, ()))

    with closing(MFS.open(tmp_path / "state")) as mfs, ThreadPoolExecutor() as pool:
        mfs.create_namespace(
            "n", "internal", processors=[Utf8TextProcessor()], embedder=OldQuery("old")
        )
        mfs.wait(mfs.upsert("n", "a.txt", b"old text"), 10)
        query = pool.submit(mfs.search, "n", "text", mode="vector", timeout=None)
        try:
            assert entered.wait(5)
            accepted = mfs.configure_namespace("n", processors=[Revised()], embedder=Model("new"))
            with mfs._condition:
                assert mfs._condition.wait_for(
                    lambda: mfs.namespace_configuration("n").active_revision == accepted.revision,
                    10,
                )
            assert mfs.search("n", "text", mode="vector").items[0].value.text == "new text"
        finally:
            release.set()
        assert query.result(10).items[0].value.text == "old text"
        mfs.wait(accepted, 10)


def test_drop_waits_for_candidate_collection_creation_to_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from mfs._index import ChunkIndex

    entered, release = threading.Event(), threading.Event()
    with closing(MFS.open(tmp_path / "state")) as mfs:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"old"), 10)
        original = ChunkIndex.recreate
        blocked: list[str] = []

        def delayed(self: ChunkIndex, *, dense_dimension: int | None) -> None:
            if dense_dimension == 2:
                blocked.append(self.collection_name)
                entered.set()
                assert release.wait(10)
            original(self, dense_dimension=dense_dimension)

        monkeypatch.setattr(ChunkIndex, "recreate", delayed)
        try:
            mfs.configure_namespace("n", embedder=Model("new"), indexing="hybrid")
            assert entered.wait(5)
            dropped = mfs.drop_namespace("n")
            mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
            mfs.upsert("n", "b.txt", b"new")
            with mfs._condition:
                assert mfs._tasks.targets[DocumentId("n", "")]["state"] == "pending"
        finally:
            release.set()
        mfs.wait(dropped, 10)
        assert not mfs._runtime.legacy_index.client.has_collection(blocked[0])
        assert mfs.search("n", "new", mode="bm25").items


def test_scan_opening_old_root_cannot_retarget_recreated_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from mfs import _sync

    old, new = tmp_path / "old", tmp_path / "new"
    old.mkdir()
    new.mkdir()
    (old / "old.txt").write_text("old")
    (new / "new.txt").write_text("new")
    entered, release = threading.Event(), threading.Event()
    with closing(MFS.open(tmp_path / "state")) as mfs, ThreadPoolExecutor() as pool:
        mfs.create_namespace("n", "external", old, processors=[Utf8TextProcessor()])
        original = _sync._case_sensitive

        def delayed(root: Path, descriptor: int) -> bool:
            if root == old:
                entered.set()
                assert release.wait(10)
            return original(root, descriptor)

        monkeypatch.setattr(_sync, "_case_sensitive", delayed)
        scan = pool.submit(mfs.sync, "n")
        try:
            assert entered.wait(5)
            mfs.drop_namespace("n")
            mfs.create_namespace("n", "external", new, processors=[Utf8TextProcessor()])
        finally:
            release.set()
        observed = scan.result(10)
        assert observed.failed and observed.failed[0].code == "SourceChanged"
        assert mfs._tasks.namespaces["n"]["root_actual"] == str(new)
        mfs.wait(mfs.sync("n"), 10)
        assert mfs.search("n", "new", mode="bm25").items
