# pyright: reportPrivateUsage=false
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import pytest
from test_lifecycle import GateEmbedder

from mfs import (
    MFS,
    DocumentId,
    GCPolicy,
    NamespaceNotFound,
    TextMatch,
    Utf8TextProcessor,
    WaitTimeout,
    _sync,
)


@pytest.mark.parametrize("kind", ["internal", "external"])
@pytest.mark.parametrize("suffix", ["txt", "md"])
def test_crlf_text_keeps_utf8_offsets_through_read_grep_search_reindex_and_reopen(
    tmp_path: Path,
    kind: Literal["internal", "external"],
    suffix: str,
) -> None:
    text = "header\r\n\u4e8c needle\r\nlast\r\n"
    source = tmp_path / "source"
    source.mkdir()
    name = "a." + suffix
    (source / name).write_bytes(b"\xef\xbb\xbf" + text.encode())
    state = tmp_path / "state"
    mfs = MFS.open(state)
    try:
        mfs.create_namespace(
            "n", kind, source if kind == "external" else None, processors=[Utf8TextProcessor()]
        )
        if kind == "internal":
            mfs.upsert("n", name, (source / name).read_bytes())
        else:
            assert mfs.sync("n").complete
        mfs.wait_ready(10)
        for _ in range(2):
            document = mfs.read(DocumentId("n", name))
            assert document is not None and document.text == text
            result = mfs.grep("n", [TextMatch("needle")], select="doc")
            assert result.items[0].value.text == text
            match = result.items[0].matches[0]
            assert text.encode()[match.text_start : match.text_end] == b"needle"
            assert match.text_start == len("header\r\n\u4e8c ".encode())
            assert match.source_location.sources == ({"kind": "lines", "start": 2, "end": 2},)
            hit = mfs.search("n", "needle", mode="bm25").items[0].value
            assert hit.text == text.encode()[hit.text_start : hit.text_end].decode()
            assert "\r\n" in hit.text
            mfs.reindex("n", timeout=10)
    finally:
        mfs.close()
    mfs = MFS.open(state)
    try:
        mfs.open_namespace("n", processors=[Utf8TextProcessor()])
        document = mfs.read(DocumentId("n", name))
        assert document is not None and document.text == text
    finally:
        mfs.close()


def test_sync_enumerates_each_directory_once_instead_of_once_per_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    for index in range(80):
        (source / f"{index}.txt").write_text("needle")
    original = _sync.files.scandir
    scans = 0

    def scandir(path: Any = None) -> Any:
        nonlocal scans
        if isinstance(path, int):
            scans += 1
        return original(path)

    mfs = MFS.open(tmp_path / "state", gc_policy=GCPolicy(enabled=False))
    try:
        mfs.create_namespace(
            "n", "external", source, processors=[Utf8TextProcessor()], indexing="off"
        )
        monkeypatch.setattr(_sync.files, "scandir", scandir)
        assert len(mfs.sync("n", verify="content").changed) == 80
        assert scans <= 3  # One filesystem case probe and one root traversal.
        scans = 0
        assert not mfs.sync("n", verify="content").changed
        assert scans <= 3
        mfs.wait_ready(10)
    finally:
        mfs.close()


def test_namespace_configuration_reads_persisted_and_pending_settings_without_binding(
    tmp_path: Path,
) -> None:
    model = GateEmbedder()
    model.release.clear()
    state = tmp_path / "state"
    mfs = MFS.open(state)
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=model)
        mfs.upsert("n", "a.txt", b"needle")
        assert model.entered.wait(5)
        mfs.configure_index("n", paused=True)
        with pytest.raises(WaitTimeout):
            mfs.reindex("n", indexing="bm25", timeout=0.01)
        config = mfs.namespace_configuration("n")
        assert config.paused and config.indexing == "hybrid"
        assert isinstance(config.pending_manifest, dict)
        pending_index = config.pending_manifest["index"]
        assert isinstance(pending_index, dict) and pending_index["dense"] is None
        assert isinstance(config.manifest, dict)
        config.manifest["processors"] = []
        assert mfs.namespace_configuration("n").manifest != config.manifest
    finally:
        model.release.set()
        mfs.close()
    mfs = MFS.open(state)
    try:
        config = mfs.namespace_configuration("n")
        assert config.paused and config.indexing == "hybrid"
        assert isinstance(config.manifest, dict) and config.manifest["processors"]
    finally:
        mfs.close()


def test_wait_follows_current_rebuild_even_when_sync_and_source_are_unchanged(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("needle")
    model = GateEmbedder()
    model.release.clear()
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "external", source, processors=[Utf8TextProcessor()])
        observed = mfs.sync("n")
        mfs.wait(observed, 10)
        identity = DocumentId("n", "a.txt")
        before = mfs.document_status(identity)
        assert before is not None
        with pytest.raises(WaitTimeout):
            mfs.reindex("n", embedder=model, indexing="hybrid", timeout=0.01)
        assert model.entered.wait(5)
        unchanged = mfs.sync("n")
        assert not unchanged.changed
        assert not unchanged.index_ready
        current = mfs.document_status(identity)
        assert current is not None and current.revision == before.revision
        assert current.indexed_revision is None
        for target in (observed, unchanged, identity, "n"):
            with pytest.raises(WaitTimeout):
                mfs.wait(target, 0.01)
        statuses = mfs.list_document_statuses("n")
        assert [status.id for status in statuses if status.id.doc_id] == [identity]
        assert mfs.scope_status("n", "a.txt").total == 1
        model.release.set()
        mfs.wait(observed, 10)
        assert mfs.search("n", "needle", mode="vector").items
    finally:
        model.release.set()
        mfs.close()


@pytest.mark.parametrize("operation", ["remove", "upsert"])
def test_schema_five_upgrade_recovers_current_work_without_historical_receipts(
    tmp_path: Path, operation: str
) -> None:
    import json
    import sqlite3
    import subprocess
    import sys

    state = tmp_path / "state"
    mfs = MFS.open(state)
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
        accepted = mfs.upsert("n", "a.txt", b"old needle", idempotency_key="first")
        mfs.wait(accepted, 10)
    finally:
        mfs.close()
    # Die after the target/tombstone transaction, before notifying the worker.
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os, sys
from pathlib import Path
from mfs import MFS, Utf8TextProcessor
mfs = MFS.open(Path(sys.argv[1]))
mfs.open_namespace('n', processors=[Utf8TextProcessor()])
mfs._tasks.remember = lambda *args: os._exit(23)
if sys.argv[2] == 'remove':
    mfs.remove('n', 'a.txt')
else:
    mfs.upsert('n', 'a.txt', b'new needle')
""",
            str(state),
            operation,
        ],
        capture_output=True,
        timeout=20,
    )
    assert child.returncode == 23, child.stderr.decode()
    # Reconstitute the retired v5 tables around the actual durable file state.
    with sqlite3.connect(state / "catalog.sqlite") as connection:
        connection.executescript("""
            CREATE TABLE runs(revision TEXT PRIMARY KEY, state TEXT NOT NULL,
                error TEXT, error_code TEXT, retryable INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE wait_operations(operation_id TEXT PRIMARY KEY,
                targets TEXT NOT NULL, complete INTEGER NOT NULL);
            CREATE TABLE wait_target_sets(id TEXT PRIMARY KEY, revisions TEXT NOT NULL);
            CREATE TABLE run_dependencies(parent TEXT NOT NULL, child TEXT NOT NULL,
                PRIMARY KEY(parent, child));
            INSERT INTO runs VALUES('historical-source', 'succeeded', NULL, NULL, 0);
            INSERT INTO wait_operations VALUES('receipt', 'set', 1);
            INSERT INTO wait_target_sets VALUES('set', '["historical-source"]');
            INSERT INTO run_dependencies VALUES('rebuild', 'historical-source');
            UPDATE operations SET value=json_set(value, '$.operation_id', 'receipt');
            PRAGMA user_version=5;
        """)
        target = json.loads(connection.execute("SELECT value FROM targets").fetchone()[0])
        assert target["state"] == "pending"
        assert target["kind"] == ("delete" if operation == "remove" else "upsert")
    mfs = MFS.open(state)
    try:
        mfs.open_namespace("n", processors=[Utf8TextProcessor()])
        mfs.wait(DocumentId("n", "a.txt"), 10)
        current = mfs.document_status(DocumentId("n", "a.txt"))
        assert current is not None and current.state == "succeeded"
        assert current.revision == target["revision"]
        assert not mfs.search("n", "old", mode="bm25").items
        document = mfs.read(DocumentId("n", "a.txt"))
        if operation == "remove":
            assert document is None
        else:
            assert document is not None and document.text == "new needle"
            assert len(mfs.search("n", "new", mode="bm25").items) == 1
        # Explicit request deduplication survives, but doesn't resurrect old file state.
        replayed = mfs.upsert("n", "a.txt", b"old needle", idempotency_key="first")
        assert replayed.revision == accepted.revision
        mfs.wait(replayed, 0)
        assert mfs.document_status(replayed.id) == current
        catalog = mfs._catalog
        assert catalog.query("PRAGMA user_version")[0][0] == 8
        assert catalog.query("SELECT count(*) FROM targets")[0][0] == 1
        assert catalog.query("SELECT count(*) FROM operations")[0][0] == 1
        tables = {
            row[0] for row in catalog.query("SELECT name FROM sqlite_schema WHERE type='table'")
        }
        assert not tables.intersection(
            {"runs", "wait_operations", "wait_target_sets", "run_dependencies"}
        )
        stored = json.loads(catalog.query("SELECT value FROM operations")[0][0])
        assert "operation_id" not in stored
        with pytest.raises(NamespaceNotFound):
            mfs.wait("receipt", 0)
        mfs.wait(mfs.drop_namespace("never-created"), 0)
    finally:
        mfs.close()
