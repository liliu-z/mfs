# pyright: reportPrivateUsage=false
from __future__ import annotations

import os
from pathlib import Path

import pytest

from mfs import MFS, Utf8TextProcessor
from mfs._platform import WindowsFiles, descriptor_change_time

pytestmark = pytest.mark.skipif(os.name != "nt", reason="requires native Windows handles")


def test_windows_handle_pins_file_and_rejects_reparse_point(tmp_path: Path) -> None:
    files = WindowsFiles()
    root = tmp_path / "root"
    root.mkdir()
    original = root / "original.txt"
    original.write_bytes(b"pinned")
    alias = root / "alias.txt"
    alias.symlink_to(original)
    descriptor = files.open(original, os.O_RDONLY)
    try:
        assert os.read(descriptor, 16) == b"pinned"
        with pytest.raises(OSError):
            original.rename(root / "renamed.txt")
        with pytest.raises(OSError):
            files.open(alias, os.O_RDONLY)
    finally:
        os.close(descriptor)
    original.rename(root / "renamed.txt")


def test_windows_native_sync_lock_reopen_and_case_rename(tmp_path: Path) -> None:
    from mfs import InstanceLocked

    root, state = tmp_path / "root", tmp_path / "state"
    root.mkdir()
    (root / "Note.txt").write_bytes(b"needle\r\n")
    mfs = MFS.open(state, processors=[Utf8TextProcessor()])
    try:
        mfs.create_namespace("n", "external", root)
        mfs.wait(mfs.sync("n"), 15)
        with pytest.raises(InstanceLocked):
            MFS.open(state, processors=[Utf8TextProcessor()])
        (root / "Note.txt").rename(root / "note.txt")
        mfs.wait(mfs.sync("n"), 15)
        assert [i.value.doc_id for i in mfs.query().items] == ["note.txt"]
        assert mfs.query(select="doc").items[0].value.text == "needle\r\n"
    finally:
        mfs.close()
    mfs = MFS.open(state, processors=[Utf8TextProcessor()])
    try:
        assert mfs.search("needle", mode="bm25").items
        (root / "note.txt").unlink()
        mfs.wait(mfs.sync("n"), 15)
        assert not mfs.search("needle", mode="bm25").items
    finally:
        mfs.close()


def test_windows_change_time_observes_restored_mtime(tmp_path: Path) -> None:
    path = tmp_path / "source.txt"
    path.write_bytes(b"before")
    files = WindowsFiles()
    descriptor = files.open(path, os.O_RDONLY)
    try:
        metadata = path.stat()
        before = descriptor_change_time(descriptor)
        path.write_bytes(b"after!")
        os.utime(path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
        assert path.stat().st_mtime_ns == metadata.st_mtime_ns
        assert descriptor_change_time(descriptor) != before
    finally:
        os.close(descriptor)
