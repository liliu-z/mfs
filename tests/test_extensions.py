# pyright: reportPrivateUsage=false
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import MappingProxyType

import pytest

from mfs import (
    MFS,
    DocumentId,
    GCPolicy,
    OperationFailed,
    ProcessedDocument,
    ProcessingContext,
    SourceMap,
    Utf8TextProcessor,
    WaitTimeout,
)
from mfs._json import JSONValue
from mfs._platform import validate_windows_relative


class ContextAdapter:
    id = "context-test"
    version = "1"
    options: JSONValue = None
    media_types: tuple[str, ...] = ("text/plain",)
    suffix_media_types: Mapping[str, str] = MappingProxyType({".txt": "text/plain"})
    workload = "light"

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.finish = False
        self.calls = 0
        self.resumed = False

    def sniff(self, head: bytes) -> str | None:
        return "text/plain" if head.startswith(b"sniff:") else None

    def process(
        self, staged_path: Path, media_type: str, context: ProcessingContext
    ) -> ProcessedDocument:
        self.calls += 1
        if context.resume_state is not None:
            assert context.resume_state == {"page": 1}
            assert context.resume_files["partial"].read_bytes() == b"checkpoint bytes"
            self.resumed = True
        else:
            partial = context.work_dir / "part"
            partial.write_bytes(b"checkpoint bytes")
            context.report_progress(1, 2, "pages")
            context.checkpoint({"page": 1}, files={"partial": partial})
        self.entered.set()
        if not self.finish:
            context.cancellation.wait(10)
            context.cancellation.check()
            raise AssertionError("test cancellation did not arrive")
        attachment = context.work_dir / "transcript.json"
        attachment.write_bytes(b'{"complete":true}')
        return ProcessedDocument(
            staged_path.read_text(), SourceMap(1, ()), {"transcript": attachment}
        )


def wait_state(mfs: MFS, identity: DocumentId, state: str, *, retired: bool = False) -> None:
    with mfs._condition:
        assert mfs._condition.wait_for(
            lambda: (
                mfs._tasks.targets[identity]["state"] == state
                and (not retired or not any(i == identity for i, _ in mfs._tasks.executing))
            ),
            10,
        )


def test_checkpoint_cancel_reopen_retry_and_atomic_artifact(tmp_path: Path) -> None:
    adapter = ContextAdapter()
    state = tmp_path / "state"
    mfs = MFS.open(state)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[adapter])
    identity = DocumentId("n", "a.txt")
    try:
        mfs.create_namespace("n", "internal", processors=[adapter])
        receipt = mfs.upsert("n", "a.txt", b"source")
        assert adapter.entered.wait(5)
        status = mfs.document_status(identity)
        assert status and status.progress and status.progress.completed == 1
        assert status.content_hash and status.source_size == 6
        assert not mfs.grep().items
        mfs.cancel(identity)
        wait_state(mfs, identity, "cancelled", retired=True)
        with pytest.raises(OperationFailed) as error:
            mfs.wait(receipt, 1)
        assert error.value.state == "cancelled"
    finally:
        mfs.close()
    adapter = ContextAdapter()
    adapter.finish = True
    mfs = MFS.open(state)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[adapter])
    try:
        assert mfs.document_status(identity).state == "cancelled"  # type: ignore[union-attr]
        mfs.retry(identity)
        mfs.wait(receipt, 10)
        assert adapter.resumed
        with mfs.open_artifact(identity, "transcript") as artifact:
            assert artifact.read() == b'{"complete":true}'
            assert artifact.snapshot_id == mfs.grep(select="doc").items[0].value.snapshot_id
        assert mfs.scope_status("n").states == {"succeeded": 1}
        assert mfs.list_document_statuses("n", path="a.txt", limit=1)[0].artifacts == (
            "transcript",
        )
    finally:
        mfs.close()


def test_cancel_gate_survives_new_source_and_reopen(tmp_path: Path) -> None:
    adapter = ContextAdapter()
    state = tmp_path / "state"
    mfs = MFS.open(state)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[adapter])
    identity = DocumentId("n", "a.txt")
    try:
        mfs.create_namespace("n", "internal", processors=[adapter])
        old = mfs.upsert("n", "a.txt", b"old")
        assert adapter.entered.wait(5)
        mfs.cancel(identity)
        wait_state(mfs, identity, "cancelled", retired=True)
        new = mfs.upsert("n", "a.txt", b"changed")
        assert mfs.document_status(identity).state == "cancelled"  # type: ignore[union-attr]
        with pytest.raises(OperationFailed, match="cancelled"):
            mfs.wait(old, 0)
    finally:
        mfs.close()
    adapter.finish = True
    mfs = MFS.open(state)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[adapter])
    try:
        with pytest.raises(OperationFailed):
            mfs.wait(new, 0)
        mfs.retry(identity)
        mfs.wait(new, 10)
        assert not adapter.resumed  # Checkpoints belong to the old revision only.
        assert mfs.grep(select="doc").items[0].value.text == "changed"
    finally:
        mfs.close()


class FailingText(Utf8TextProcessor):
    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
        if staged_path.read_bytes() == b"bad":
            raise ValueError("bad input")
        return super().process(staged_path, media_type)


def test_wait_is_scoped_and_durable_and_receipts_use_global_ready(tmp_path: Path) -> None:
    state = tmp_path / "state"
    mfs = MFS.open(state)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[FailingText()])
    try:
        mfs.create_namespace("n", "internal", processors=[FailingText()])
        good = mfs.upsert("n", "good.txt", b"needle", idempotency_key="first")
        mfs.wait(good, 10)
        bad = mfs.upsert("n", "bad.txt", b"bad")
        wait_state(mfs, bad.id, "failed")
        assert not mfs.upsert("n", "good.txt", b"needle").index_ready
        assert mfs.upsert("n", "good.txt", b"needle", idempotency_key="first") == good
        status = mfs.document_status(bad.id)
        assert status and status.error_detail and status.error_detail.code == "ValueError"
        assert not status.error_detail.retryable
        mfs.wait(good, 0)
        deletion = mfs.remove("n", "good.txt")
        mfs.wait(deletion, 10)
        assert not mfs.search("needle", mode="bm25", consistency="eventual").items
        mfs.wait(good, 0)  # Successful historical operations stay successful.
        with pytest.raises(OperationFailed):
            mfs.wait(bad, 0)
        with pytest.raises(WaitTimeout):
            mfs.wait_ready(0)
    finally:
        mfs.close()
    mfs = MFS.open(state)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[FailingText()])
    try:
        mfs.wait(good, 0)
        mfs.wait(deletion, 0)
        drop = mfs.drop_namespace("n")
        mfs.create_namespace("n", "internal", processors=[FailingText()])
        fresh = mfs.upsert("n", "fresh.txt", b"fresh")
        mfs.wait(drop, 10)
        mfs.wait(fresh, 10)
        assert mfs.search("fresh", mode="bm25").items
    finally:
        mfs.close()


class ReusableText(Utf8TextProcessor):
    cache_scope = "content"

    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
        self.calls += 1
        return super().process(staged_path, media_type)


class CountingEmbedder:
    embedding_space = "extension-test"
    dimension = 2

    def __init__(self) -> None:
        self.calls = 0

    def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        self.calls += 1
        return [[1.0, 0.5] for _ in texts]

    def embed_query(self, text: str) -> Sequence[float]:
        return [1.0, 0.5]


def test_reuse_across_sync_calls_preserves_ids_and_reprocess_bypasses_process(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "a.txt").write_text("reuse needle")
    processor, embedder = ReusableText(), CountingEmbedder()
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[processor], embedder=embedder)
    try:
        mfs.create_namespace("n", "external", root, processors=[processor], embedder=embedder)
        mfs.wait(mfs.sync("n"), 10)
        (root / "a.txt").unlink()
        mfs.wait(mfs.sync("n"), 10)
        (root / "b.txt").write_text("reuse needle")
        mfs.wait(mfs.sync("n"), 10)
        assert processor.calls == 1
        assert embedder.calls == 2  # Last live vector was deleted along with a.txt.
        (root / "c.txt").write_text("reuse needle")
        mfs.wait(mfs.sync("n"), 10)
        assert {i.value.doc_id for i in mfs.grep().items} == {"b.txt", "c.txt"}
        assert {
            i.value.doc_id for i in mfs.search("needle", mode="bm25", select="doc_id").items
        } == {"b.txt", "c.txt"}
        mfs.wait(mfs.reprocess(DocumentId("n", "b.txt")), 10)
        assert processor.calls == 2
        assert embedder.calls == 2
    finally:
        mfs.close()


def test_gc_pins_open_artifacts_and_retains_cancelled_checkpoints(tmp_path: Path) -> None:
    processor = ContextAdapter()
    processor.finish = True
    policy = GCPolicy(enabled=False, idle_seconds=0, grace_seconds=0, batch_files=128)
    mfs = MFS.open(tmp_path / "state", gc_policy=policy)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[processor])
    identity = DocumentId("n", "a.txt")
    try:
        mfs.create_namespace("n", "internal", processors=[processor])
        mfs.wait(mfs.upsert("n", "a.txt", b"source"), 10)
        handle = mfs.open_artifact(identity, "transcript")
        try:
            mfs.wait(mfs.remove("n", "a.txt"), 10)
            assert mfs.collect_garbage().busy
            assert handle.read() == b'{"complete":true}'
        finally:
            handle.close()
        wait_state(mfs, identity, "succeeded", retired=True)
        deadline = time.monotonic() + 5
        while True:
            report = mfs.collect_garbage()
            assert report.error is None
            remaining = [
                p
                for folder in ("objects", "artifacts", "work")
                for p in (tmp_path / "state" / folder).iterdir()
            ]
            if not remaining:
                break
            assert time.monotonic() < deadline, (report, remaining)
            threading.Event().wait(0.01)  # Busy maintenance is retried by the host.

        processor.finish = False
        processor.entered.clear()
        mfs.upsert("n", "a.txt", b"retained")
        assert processor.entered.wait(5)
        mfs.cancel(identity)
        wait_state(mfs, identity, "cancelled", retired=True)
        checkpoint = mfs._tasks.targets[identity]["checkpoint"]["files"]["partial"]
        for _ in range(4):
            assert mfs.collect_garbage().error is None
        assert (mfs._path / checkpoint).read_bytes() == b"checkpoint bytes"
        processor.finish = True
        mfs.retry(identity)
        mfs.wait_ready(10)
        assert processor.resumed
    finally:
        mfs.close()


def test_unsupported_sniff_is_bounded_and_case_rename_retires_old_spelling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "unsupported.bin").write_bytes(b"x" * 2_000_000)
    (root / "Note.txt").write_text("needle")
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    sizes: list[int] = []
    original = mfs._stage_descriptor

    def stage(descriptor: int, source: Path):  # type: ignore[no-untyped-def]
        sizes.append(os.fstat(descriptor).st_size)
        return original(descriptor, source)

    monkeypatch.setattr(mfs, "_stage_descriptor", stage)

    # This deterministic test also exercises the comparison rule on case-sensitive CI.
    def insensitive(*args: object) -> bool:
        return False

    monkeypatch.setattr("mfs._sync._case_sensitive", insensitive)
    try:
        mfs.create_namespace("n", "external", root, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("n"), 10)
        (root / "Note.txt").rename(root / "note.txt")
        receipt = mfs.sync("n")
        mfs.wait(receipt, 10)
        assert receipt.removed == (DocumentId("n", "Note.txt"),)
        assert [i.value.doc_id for i in mfs.grep().items] == ["note.txt"]
        assert [
            i.value.doc_id for i in mfs.search("needle", mode="bm25", select="doc_id").items
        ] == ["note.txt"]
        assert sizes == [6, 6]
        (root / "note.txt").write_text("changed")
        mfs.wait(mfs.sync("n"), 10)
        assert not mfs.search("needle", mode="bm25").items
    finally:
        mfs.close()


class NativeAdapter(ContextAdapter):
    def process(
        self, staged_path: Path, media_type: str, context: ProcessingContext
    ) -> ProcessedDocument:
        self.entered.set()
        context.run_process([sys.executable, "-c", "import time; time.sleep(120)"])
        return ProcessedDocument("finished", SourceMap(1, ()))


def test_managed_process_cancel_releases_single_worker(tmp_path: Path) -> None:
    adapter = NativeAdapter()
    adapter.workload = "heavy"
    adapter.media_types = ("application/test",)
    adapter.suffix_media_types = {".native": "application/test"}
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[adapter, Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal", processors=[adapter, Utf8TextProcessor()])
        native = mfs.upsert("n", "long.native", b"input")
        assert adapter.entered.wait(5)
        fast = mfs.upsert("n", "fast.txt", b"fast")
        with pytest.raises(WaitTimeout):
            mfs.wait(fast, 0)
        with pytest.raises(WaitTimeout):
            mfs.wait(native, 0)
        started = time.monotonic()
        mfs.cancel(native.id)
        wait_state(mfs, native.id, "cancelled", retired=True)
        assert time.monotonic() - started < 5
        mfs.wait(fast, 10)
    finally:
        mfs.close()


@pytest.mark.parametrize(
    "path", ["C:/escape", "foo:stream", "CON", "dir/NUL.txt", "trailing.", "trailing "]
)
def test_windows_rejects_path_aliases(path: str) -> None:
    from mfs import InvalidPath

    with pytest.raises(InvalidPath):
        validate_windows_relative(path)


def test_checkpoint_survives_process_termination(tmp_path: Path) -> None:
    state = tmp_path / "state"
    script = """
import os, sys
from pathlib import Path
from types import MappingProxyType
from mfs import MFS, SourceMap, ProcessedDocument
class P:
    id="crash-checkpoint"; version="1"; options={}
    media_types=("text/plain",); suffix_media_types={".txt":"text/plain"}
    def sniff(self, head): return None
    def process(self, path, media, context):
        if context.resume_state:
            assert context.resume_files["page"].read_bytes()==b"page"
            return ProcessedDocument("recovered",SourceMap(1,()))
        f=context.work_dir/"page"; f.write_bytes(b"page")
        context.checkpoint({"page":1},files={"page":f})
        os._exit(29)
m=MFS.open(Path(sys.argv[1]))
for registered in m.list_namespaces():
    m.open_namespace(registered.namespace, processors=[P()])
if not m.list_namespaces():
    m.create_namespace('n', 'internal', processors=[P()])
m.upsert("n","a.txt",b"source")
m.wait_ready(10)
assert m.grep(select="doc").items[0].value.text=="recovered"
m.close()
"""
    first = subprocess.run(
        [sys.executable, "-c", script, str(state)], capture_output=True, timeout=25
    )
    assert first.returncode == 29, first.stderr.decode()
    second = subprocess.run(
        [sys.executable, "-c", script, str(state)], capture_output=True, timeout=25
    )
    assert second.returncode == 0, second.stderr.decode()


def test_sync_wait_includes_unchanged_pending_and_descendant_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "root"
    (root / "folder.txt").mkdir(parents=True)
    (root / "folder.txt" / "child.txt").write_text("old descendant")
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    entered, release = threading.Event(), threading.Event()
    try:
        mfs.create_namespace("n", "external", root, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("n"), 10)
        (root / "folder.txt" / "child.txt").unlink()
        (root / "folder.txt").rmdir()
        (root / "folder.txt").write_text("replacement")
        original = mfs._runtime.index("n").delete_document

        def delayed(identity: DocumentId, *, incarnation: str | None = None) -> None:
            entered.set()
            assert release.wait(10)
            original(identity, incarnation=incarnation)

        monkeypatch.setattr(mfs._runtime.index("n"), "delete_document", delayed)
        receipt = mfs.sync("n")
        assert entered.wait(5)
        with pytest.raises(WaitTimeout):
            mfs.wait(receipt, 0)
        # A second scan must include already-accepted deletion work in its scope.
        unchanged = mfs.sync("n")
        with pytest.raises(WaitTimeout):
            mfs.wait(unchanged, 0)
        release.set()
        mfs.wait(receipt, 10)
        mfs.wait(unchanged, 10)
        assert not mfs.search("descendant", mode="bm25", consistency="eventual").items
    finally:
        release.set()
        mfs.close()


def test_delete_runs_after_rejected_adapter_binding(tmp_path: Path) -> None:
    from mfs import DefaultChunker, NamespaceCompatibilityError

    state = tmp_path / "state"
    mfs = MFS.open(state)
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "a.txt", b"retire"), 10)
    finally:
        mfs.close()
    chunker = DefaultChunker()
    chunker.version = "another-config"
    mfs = MFS.open(state)
    try:
        with pytest.raises(NamespaceCompatibilityError):
            mfs.open_namespace("n", processors=[Utf8TextProcessor()], chunker=chunker)
        mfs.wait(mfs.remove("n", "a.txt"), 10)
        assert not mfs.search("retire", mode="bm25", consistency="eventual").items
    finally:
        mfs.close()


def test_corrupt_weak_cache_recomputes_and_path_dependent_adapters_do_not_share(
    tmp_path: Path,
) -> None:
    processor = ReusableText()
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[processor])
    try:
        mfs.create_namespace("n", "internal", processors=[processor])
        mfs.wait(mfs.upsert("n", "a.txt", b"cache"), 10)
        row = mfs._catalog.connection.execute(
            "SELECT path FROM cache WHERE key LIKE 'process:%'"
        ).fetchone()
        (mfs._path / row[0]).write_text('{"text":"tampered"}')
        mfs.wait(mfs.upsert("n", "b.txt", b"cache"), 10)
        assert processor.calls == 2
        assert all(i.value.text == "cache" for i in mfs.grep(select="doc").items)
    finally:
        mfs.close()

    class PathAdapter(ContextAdapter):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext
        ) -> ProcessedDocument:
            self.calls += 1
            return ProcessedDocument(context.document_id.doc_id, SourceMap(1, ()))

    adapter = PathAdapter()
    mfs = MFS.open(tmp_path / "path-state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[adapter])
    try:
        mfs.create_namespace("n", "internal", processors=[adapter])
        mfs.wait(mfs.upsert("n", "first.txt", b"same"), 10)
        mfs.wait(mfs.upsert("n", "second.txt", b"same"), 10)
        assert adapter.calls == 2
        assert {i.value.text for i in mfs.grep(select="doc").items} == {"first.txt", "second.txt"}
    finally:
        mfs.close()


def test_gc_recovers_claimed_deletion_and_never_changes_task_state(tmp_path: Path) -> None:
    import sqlite3

    state = tmp_path / "state"
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"untouched")
    policy = GCPolicy(enabled=False, grace_seconds=0, idle_seconds=0)
    mfs = MFS.open(state, gc_policy=policy)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        receipt = mfs.upsert("n", "a.txt", b"keep")
        mfs.wait(receipt, 10)
        orphan = state / "artifacts" / "orphan"
        orphan.write_bytes(b"reclaim")
        alias = state / "artifacts" / "alias"
        alias.symlink_to(outside)
        missing = "artifacts/already-unlinked"
        with mfs._catalog.transaction():
            mfs._catalog.register_artifact("artifacts/orphan")
            mfs._catalog.register_artifact("artifacts/alias")
            mfs._catalog.register_artifact(missing)
    finally:
        mfs.close()
    with sqlite3.connect(state / "catalog.sqlite") as connection:
        connection.execute(
            "UPDATE artifacts SET state='deleting' WHERE path IN (?,?)",
            ("artifacts/orphan", missing),
        )
    mfs = MFS.open(state, gc_policy=policy)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        before = mfs.document_status(receipt.id)
        deadline = time.monotonic() + 5
        while orphan.exists() or alias.is_symlink():
            report = mfs.collect_garbage()
            assert report.error is None
            assert time.monotonic() < deadline
            if report.busy:
                time.sleep(0.01)
        assert not orphan.exists()
        assert not alias.is_symlink()
        assert outside.read_bytes() == b"untouched"
        assert mfs.document_status(receipt.id) == before
        document = mfs.read(DocumentId("n", "a.txt"))
        assert document is not None and document.original == b"keep"
        mfs.wait(receipt, 0)
    finally:
        mfs.close()


def test_checkpoint_keeps_single_file_order(tmp_path: Path) -> None:
    from mfs import UnderPath

    entered, checkpoint = threading.Event(), threading.Event()
    order: list[str] = []

    class PriorityAdapter(ContextAdapter):
        def process(
            self, staged_path: Path, media_type: str, context: ProcessingContext
        ) -> ProcessedDocument:
            name = context.document_id.doc_id
            if name == "background.txt" and context.resume_state is None:
                entered.set()
                assert checkpoint.wait(10)
                context.checkpoint({"saved": True})
            order.append(name)
            return ProcessedDocument(name, SourceMap(1, ()))

    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[PriorityAdapter()])
    try:
        mfs.create_namespace("n", "internal", processors=[PriorityAdapter()])
        mfs.upsert("n", "background.txt", b"background")
        assert entered.wait(5)
        mfs.set_active_scopes([UnderPath("n", "active.txt")])
        mfs.upsert("n", "active.txt", b"active")
        checkpoint.set()
        mfs.wait_ready(10)
        assert order == ["background.txt", "active.txt"]
    finally:
        checkpoint.set()
        mfs.close()
