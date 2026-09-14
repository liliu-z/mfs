from __future__ import annotations

from pathlib import Path

from ._platform import fsync_directory
from .errors import CorruptState

MARKER = ".mfs-initializing"
DIRECTORIES = ("objects", "artifacts", "staging", "work", "namespaces")


def prepare(path: Path) -> None:
    """Mark ownership before the first lock/catalog file can be left by a crash."""
    path.mkdir(parents=True, exist_ok=True)
    if not any(path.iterdir()):
        (path / MARKER).mkdir(exist_ok=True)
        fsync_directory(path)
        fsync_directory(path.parent)
    if (path / "catalog.sqlite").is_file():
        return
    marker = path / MARKER
    if not marker.is_dir() or marker.is_symlink() or any(marker.iterdir()):
        raise CorruptState("non-empty mfs_path has no recognizable catalog or bootstrap")
    for entry in path.iterdir():
        if entry.name == MARKER:
            continue
        if entry.is_symlink():
            raise CorruptState("bootstrap contains an unexpected symbolic link")
        if entry.name in DIRECTORIES and entry.is_dir() and not any(entry.iterdir()):
            continue
        if entry.name in ("LOCK", "PROCESS_LOCK") and entry.is_file():
            continue
        raise CorruptState(f"bootstrap contains an unexpected entry: {entry.name}")


def finish(path: Path) -> None:
    marker = path / MARKER
    if marker.exists():
        marker.rmdir()
        fsync_directory(path)
