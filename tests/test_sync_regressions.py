# pyright: reportPrivateUsage=false
from __future__ import annotations

import os
import shutil
from pathlib import Path

from test_lifecycle import CountingProcessor, GateEmbedder, wait_state

from mfs import MFS, DocumentId, SyncPolicy, TextMatch, Utf8TextProcessor


def test_ignore_applies_to_root_directory_and_exact_file(tmp_path: Path) -> None:
    root = tmp_path / "files"
    (root / "ignored").mkdir(parents=True)
    (root / "ignored/a.txt").write_text("excluded")
    path = tmp_path / "state"
    original = MFS.open(path, processors=[Utf8TextProcessor()])
    try:
        original.create_namespace("n", "external", root)
        original.sync("n")
        original.wait_ready(10)
    finally:
        original.close()
    mfs = MFS.open(
        path, processors=[Utf8TextProcessor()], sync_policy=SyncPolicy(exclude_globs=("ignored",))
    )
    try:
        for requested in ("ignored/a.txt", "ignored", "."):
            report = mfs.sync("n", requested)
            assert report.complete and not report.changed
            assert any(item.reason == "excluded" for item in report.skipped)
            mfs.wait_ready(10)
            assert not mfs.query().items
    finally:
        mfs.close()


def test_directory_replacement_preserves_children_through_process_and_dense_failures(
    tmp_path: Path,
) -> None:
    root = tmp_path / "files"
    (root / "node.txt").mkdir(parents=True)
    (root / "node.txt/old.txt").write_text("old content")
    embedder = GateEmbedder()
    mfs = MFS.open(tmp_path / "state", processors=[Utf8TextProcessor()], embedder=embedder)
    try:
        mfs.create_namespace("n", "external", root)
        mfs.sync("n")
        mfs.wait_ready(10)
        shutil.rmtree(root / "node.txt")
        (root / "node.txt").write_bytes(b"\xff")
        mfs.sync("n")
        wait_state(mfs, DocumentId("n", "node.txt"), "failed")
        assert mfs.query([TextMatch("old")]).items[0].value.doc_id == "node.txt/old.txt"
        assert mfs.search("old", mode="bm25", consistency="eventual").items
        (root / "node.txt").write_text("new content")
        embedder.fail = True
        mfs.sync("n", "node.txt")
        wait_state(mfs, DocumentId("n", "node.txt"), "failed")
        assert [item.value.doc_id for item in mfs.query().items] == ["node.txt"]
        assert mfs.query([TextMatch("new")]).items
        assert mfs.search("old", mode="bm25", consistency="eventual").items
        assert not mfs.search("new", mode="bm25", consistency="eventual").items
        embedder.fail = False
        mfs.retry(DocumentId("n", "node.txt"))
        mfs.wait_ready(10)
        assert mfs.search("new", mode="bm25").items
        assert not mfs.search("old", mode="bm25").items
    finally:
        mfs.close()


def test_content_verification_detects_restored_stat_without_reprocessing_unchanged(
    tmp_path: Path,
) -> None:
    root = tmp_path / "files"
    root.mkdir()
    source = root / "a.txt"
    source.write_text("first")
    processor = CountingProcessor()
    mfs = MFS.open(tmp_path / "state", processors=[processor])
    try:
        mfs.create_namespace("n", "external", root)
        mfs.sync("n")
        mfs.wait_ready(10)
        initial = source.stat()
        source.write_text("other")
        os.utime(source, ns=(initial.st_atime_ns, initial.st_mtime_ns))
        assert not mfs.sync("n").changed
        assert mfs.query(select="doc").items[0].value.text == "first"
        assert mfs.sync("n", verify="content").changed
        mfs.wait_ready(10)
        assert mfs.query(select="doc").items[0].value.text == "other"
        assert not mfs.sync("n", verify="content").changed
        assert not mfs.sync("n", "a.txt").changed
        assert processor.calls == 2
    finally:
        mfs.close()


def test_case_alias_reconciliation_uses_volume_identity(tmp_path: Path) -> None:
    root = tmp_path / "files"
    (root / "Sub").mkdir(parents=True)
    (root / "Sub/a.txt").write_text("content")
    insensitive = (root / "sub").exists()
    mfs = MFS.open(tmp_path / "state", processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "external", root)
        mfs.sync("n")
        mfs.wait_ready(10)
        (root / "Sub/a.txt").unlink()
        report = mfs.sync("n", "sub")
        assert report.complete
        if insensitive:
            assert report.removed == (DocumentId("n", "Sub/a.txt"),)
            assert not mfs.query().items
        else:
            assert not report.removed
            assert mfs.query().items[0].value.doc_id == "Sub/a.txt"
            assert mfs.sync("n", "Sub").removed
    finally:
        mfs.close()


def test_root_symlink_retarget_forces_full_reconcile_and_broken_root_preserves_data(
    tmp_path: Path,
) -> None:
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    (first / "old.txt").write_text("old")
    (first / "same.txt").write_text("first")
    (second / "new.txt").write_text("new")
    (second / "same.txt").write_text("other")
    metadata = (first / "same.txt").stat()
    os.utime(second / "same.txt", ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
    alias = tmp_path / "alias"
    alias.symlink_to(first, target_is_directory=True)
    mfs = MFS.open(tmp_path / "state", processors=[Utf8TextProcessor()])
    try:
        assert mfs.create_namespace("n", "external", alias).root == alias
        mfs.sync("n")
        mfs.wait_ready(10)
        alias.unlink()
        alias.symlink_to(second, target_is_directory=True)
        report = mfs.sync("n", "same.txt")
        assert report.complete and report.removed == (DocumentId("n", "old.txt"),)
        mfs.wait_ready(10)
        assert [i.value.doc_id for i in mfs.query().items] == ["new.txt", "same.txt"]
        assert mfs.query([TextMatch("other")]).items
        alias.unlink()
        alias.symlink_to(tmp_path / "missing", target_is_directory=True)
        report = mfs.sync("n")
        assert not report.complete and not report.removed
        assert len(mfs.query().items) == 2
    finally:
        mfs.close()


def test_internal_symlinks_are_contained_deduplicated_and_not_directory_traversed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "files"
    (root / "sub").mkdir(parents=True)
    (root / "sub/real.txt").write_text("inside")
    (tmp_path / "outside.txt").write_text("outside")
    (root / "alias.txt").symlink_to(root / "sub/real.txt")
    (root / "outside.txt").symlink_to(tmp_path / "outside.txt")
    (root / "directory").symlink_to(root / "sub", target_is_directory=True)
    (root / "loop").symlink_to(root / "loop")
    processor = CountingProcessor()
    mfs = MFS.open(tmp_path / "state", processors=[processor])
    try:
        mfs.create_namespace("n", "external", root)
        assert mfs.sync("n").complete
        mfs.wait_ready(10)
        assert [i.value.doc_id for i in mfs.query().items] == ["sub/real.txt"]
        assert processor.calls == 1
        assert not mfs.sync("n", "alias.txt").changed
        report = mfs.sync("n", "directory/real.txt")
        assert not report.complete and not report.removed
        assert not mfs.query([TextMatch("outside")]).items
    finally:
        mfs.close()
