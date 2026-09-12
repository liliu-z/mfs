# pyright: reportPrivateUsage=false
from __future__ import annotations

import os
import shutil
from pathlib import Path

from test_lifecycle import CountingProcessor, GateEmbedder, wait_state

from mfs import MFS, DocumentId, IgnoreRule, TextMatch, Utf8TextProcessor


def test_ignore_applies_to_root_directory_and_exact_file(tmp_path: Path) -> None:
    root = tmp_path / "files"
    (root / "ignored").mkdir(parents=True)
    (root / "ignored/a.txt").write_text("excluded")
    path = tmp_path / "state"
    original = MFS.open(path)
    for registered in original.list_namespaces():
        original.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        original.create_namespace("n", "external", root, processors=[Utf8TextProcessor()])
        original.sync("n")
        original.wait_ready(10)
    finally:
        original.close()
    mfs = MFS.open(path)
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        mfs.update_rules(
            "n", expected_revision=mfs.rules("n").revision, add=[IgnoreRule("ignored", "ignored/")]
        )
        for requested in ("ignored/a.txt", "ignored", "."):
            report = mfs.sync("n", requested)
            assert report.complete and not report.changed
            assert any(item.reason == "excluded" for item in report.skipped)
            mfs.wait_ready(10)
            assert not mfs.grep().items
    finally:
        mfs.close()


def test_directory_replacement_revokes_children_even_when_new_processing_fails(
    tmp_path: Path,
) -> None:
    root = tmp_path / "files"
    (root / "node.txt").mkdir(parents=True)
    (root / "node.txt/old.txt").write_text("old content")
    embedder = GateEmbedder()
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(
            registered.namespace, processors=[Utf8TextProcessor()], embedder=embedder
        )
    try:
        mfs.create_namespace(
            "n", "external", root, processors=[Utf8TextProcessor()], embedder=embedder
        )
        mfs.sync("n")
        mfs.wait_ready(10)
        shutil.rmtree(root / "node.txt")
        (root / "node.txt").write_bytes(b"\xff")
        mfs.sync("n")
        wait_state(mfs, DocumentId("n", "node.txt"), "failed")
        assert not mfs.grep([TextMatch("old")]).items
        assert not mfs.search("old", mode="bm25", consistency="eventual").items
        (root / "node.txt").write_text("new content")
        embedder.fail = True
        mfs.sync("n", "node.txt")
        wait_state(mfs, DocumentId("n", "node.txt"), "failed")
        assert [item.value.doc_id for item in mfs.grep().items] == ["node.txt"]
        assert mfs.grep([TextMatch("new")]).items
        assert not mfs.search("old", mode="bm25", consistency="eventual").items
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
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[processor])
    try:
        mfs.create_namespace("n", "external", root, processors=[processor])
        mfs.sync("n")
        mfs.wait_ready(10)
        initial = source.stat()
        source.write_text("other")
        os.utime(source, ns=(initial.st_atime_ns, initial.st_mtime_ns))
        assert not mfs.sync("n").changed
        assert mfs.grep(select="doc").items[0].value.text == "other"
        assert mfs.search("first", mode="bm25").items  # Stat-only sync did not rebuild.
        assert mfs.sync("n", verify="content").changed
        mfs.wait_ready(10)
        assert mfs.grep(select="doc").items[0].value.text == "other"
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
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "external", root, processors=[Utf8TextProcessor()])
        mfs.sync("n")
        mfs.wait_ready(10)
        (root / "Sub/a.txt").unlink()
        report = mfs.sync("n", "sub")
        assert report.complete
        if insensitive:
            assert report.removed == (DocumentId("n", "Sub/a.txt"),)
            assert not mfs.grep().items
        else:
            assert not report.removed
            assert mfs.grep().items[0].value.doc_id == "Sub/a.txt"
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
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[Utf8TextProcessor()])
    try:
        assert (
            mfs.create_namespace("n", "external", alias, processors=[Utf8TextProcessor()]).root
            == alias
        )
        mfs.sync("n")
        mfs.wait_ready(10)
        alias.unlink()
        alias.symlink_to(second, target_is_directory=True)
        report = mfs.sync("n", "same.txt")
        assert report.complete and report.removed == (DocumentId("n", "old.txt"),)
        mfs.wait_ready(10)
        assert [i.value.doc_id for i in mfs.grep().items] == ["new.txt", "same.txt"]
        assert mfs.grep([TextMatch("other")]).items
        alias.unlink()
        alias.symlink_to(tmp_path / "missing", target_is_directory=True)
        report = mfs.sync("n")
        assert not report.complete and not report.removed
        assert len(mfs.grep().items) == 2
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
    mfs = MFS.open(tmp_path / "state")
    for registered in mfs.list_namespaces():
        mfs.open_namespace(registered.namespace, processors=[processor])
    try:
        mfs.create_namespace("n", "external", root, processors=[processor])
        assert mfs.sync("n").complete
        mfs.wait_ready(10)
        assert [i.value.doc_id for i in mfs.grep().items] == ["sub/real.txt"]
        assert processor.calls == 1
        assert not mfs.sync("n", "alias.txt").changed
        report = mfs.sync("n", "directory/real.txt")
        assert not report.complete and not report.removed
        assert not mfs.grep([TextMatch("outside")]).items
    finally:
        mfs.close()
