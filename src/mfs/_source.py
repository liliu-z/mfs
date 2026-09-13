from __future__ import annotations

import os
import stat
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO

import blake3

from ._platform import descriptor_change_time, windows_files
from .errors import SourceChanged, SourceUnavailable


@contextmanager
def open_regular(path: Path) -> Generator[BinaryIO]:
    """Open a canonical reference without following replaced parent/leaf links.

    Both source paths and explicit borrowed outputs are stored as absolute resolved
    paths. Walk from the filesystem anchor; resolving again would authorize a new
    symlink target. Nonblocking open plus fstat rejects FIFOs before any read.
    """
    descriptor: int | None = None
    files = windows_files() if os.name == "nt" else os
    try:
        if not path.is_absolute() or ".." in path.parts:
            raise SourceUnavailable("text reference must be an absolute canonical path")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        descriptor = files.open(path.anchor, flags | getattr(os, "O_DIRECTORY", 0))
        for index, part in enumerate(path.parts[1:], 1):
            directory = index < len(path.parts) - 1
            following = files.open(
                part,
                flags | (getattr(os, "O_DIRECTORY", 0) if directory else 0),
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = following
            metadata = os.fstat(descriptor)
            if not (
                stat.S_ISDIR(metadata.st_mode) if directory else stat.S_ISREG(metadata.st_mode)
            ):
                raise SourceUnavailable("text reference is not a regular file")
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise SourceUnavailable("text reference is not a regular file")
        if os.name == "nt" and files.path(descriptor) != path:
            raise SourceUnavailable("text reference moved while opening")
        stream = os.fdopen(descriptor, "rb")
        descriptor = None
        with stream:
            yield stream
    except OSError as error:
        raise SourceUnavailable(f"cannot open search text {path}: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


class SourceGuard:
    """Validate the borrowed input across a processing unit without copying it."""

    def __init__(self, path: Path, content_hash: str, observed: dict[str, Any]) -> None:
        self.path, self.content_hash = path, content_hash
        self.observed = (observed.get("size"), observed.get("mtime_ns"))
        with open_regular(path) as stream:
            self.stamp = self._stamp(stream)
            self._verify(stream, self.stamp)

    @staticmethod
    def _stamp(stream: BinaryIO) -> tuple[int, int, int, int, int]:
        descriptor = stream.fileno()
        value = os.fstat(descriptor)
        return (
            value.st_dev,
            value.st_ino,
            value.st_size,
            value.st_mtime_ns,
            descriptor_change_time(descriptor),
        )

    def _verify(self, stream: BinaryIO, stamp: tuple[int, int, int, int, int]) -> None:
        digest = blake3.blake3()
        while block := stream.read(1024 * 1024):
            digest.update(block)
        path_stat = self.path.stat()
        if (
            self._stamp(stream) != stamp
            or (path_stat.st_dev, path_stat.st_ino) != stamp[:2]
            or digest.hexdigest() != self.content_hash
        ):
            raise SourceChanged("external input changed during processing; sync again")

    def check(self, observed: dict[str, Any]) -> None:
        with open_regular(self.path) as stream:
            stamp = self._stamp(stream)
            if stamp == self.stamp:
                return
            # A same-content sync may explicitly refresh stat during an attempt.
            # Unobserved changes, including an in-place change-and-restore, invalidate it.
            if (
                stamp[:2] != self.stamp[:2]
                or (stamp[2], stamp[3]) != (observed.get("size"), observed.get("mtime_ns"))
                or (stamp[2], stamp[3]) == self.observed
            ):
                raise SourceChanged("external input changed during processing; sync again")
            self._verify(stream, stamp)
            self.stamp = stamp
            self.observed = stamp[2], stamp[3]
