# pyright: reportPrivateUsage=false
from __future__ import annotations

import contextlib
import os
import shutil
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, cast

import blake3

from ._json import canonical_json, load_json
from .errors import CorruptState, InvalidQuery
from .types import GCPolicy, GCReport

if TYPE_CHECKING:
    from ._core import MFS


class ArtifactHandle:
    """A pinned immutable artifact. Close it before closing its MFS instance."""

    def __init__(self, stream: BinaryIO, snapshot_id: str, release: Any) -> None:
        self.stream = stream
        self.snapshot_id = snapshot_id
        self._release = release

    def read(self, size: int = -1) -> bytes:
        return self.stream.read(size)

    def close(self) -> None:
        try:
            self.stream.close()
        finally:
            if self._release is not None:
                release, self._release = self._release, None
                release()

    def __enter__(self) -> ArtifactHandle:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class ArtifactStore:
    def __init__(self, mfs: MFS, policy: GCPolicy) -> None:
        self.mfs = mfs
        self.policy = policy
        self._collect_lock = threading.Lock()
        self._inventory: Iterator[Path] | None = None
        self.last_report = GCReport()

    @staticmethod
    def key(kind: str, value: Any) -> str:
        return kind + ":" + blake3.blake3(canonical_json(value)).hexdigest()

    def cached(self, key: str) -> Any | None:
        catalog = self.mfs._catalog
        row = catalog.connection.execute(
            "SELECT cache.path,cache.digest FROM cache JOIN artifacts USING(path) "
            "WHERE key=? AND state='live'",
            (key,),
        ).fetchone()
        if row is None:
            return None
        try:
            path = self.mfs._path / str(row[0])
            if path.parent != self.mfs._path / "artifacts" or path.is_symlink():
                raise CorruptState("invalid cache artifact path")
            data = path.read_bytes()
            if blake3.blake3(data).hexdigest() != row[1]:
                raise CorruptState("cached artifact checksum mismatch")
            return load_json(data.decode("utf-8"))
        except (CorruptState, OSError, ValueError):
            with catalog.transaction():
                catalog.connection.execute("DELETE FROM cache WHERE key=?", (key,))
            return None

    def cache(self, key: str, path: str) -> None:
        catalog = self.mfs._catalog
        digest = blake3.blake3((self.mfs._path / path).read_bytes()).hexdigest()
        with catalog.transaction():
            catalog.connection.execute(
                "INSERT INTO cache VALUES(?,?,?) ON CONFLICT(key) "
                "DO UPDATE SET path=excluded.path,digest=excluded.digest",
                (key, path, digest),
            )

    def copy_files(self, work_dir: Path, files: Mapping[str, Path]) -> dict[str, str]:
        result: dict[str, str] = {}
        for name, file in files.items():
            if (
                not isinstance(cast(object, name), str)
                or not name
                or len(name.encode()) > 255
                or "\0" in name
            ):
                raise InvalidQuery("artifact names must be 1..255 UTF-8 bytes without NUL")
            candidate = Path(file)
            if not candidate.is_absolute():
                candidate = work_dir / candidate
            # No symlink in any component, including a parent resolving back inside.
            try:
                relative = candidate.relative_to(work_dir)
            except ValueError as error:
                raise InvalidQuery("artifact must be under the attempt work_dir") from error
            if ".." in relative.parts or any(
                (work_dir.joinpath(*relative.parts[:n])).is_symlink()
                for n in range(1, len(relative.parts) + 1)
            ):
                raise InvalidQuery("artifact paths cannot traverse links or '..'")
            if not candidate.is_file():
                raise InvalidQuery("artifact must be a regular file")
            path = "artifacts/" + uuid.uuid4().hex + ".bin"
            with self.mfs._catalog.transaction():
                self.mfs._catalog.register_artifact(path)
            destination = self.mfs._path / path
            with candidate.open("rb") as source, destination.open("xb") as target:
                shutil.copyfileobj(source, target, 1024 * 1024)
                target.flush()
                os.fsync(target.fileno())
            # Read-only is a contract as well as a useful guard against accidental edits.
            destination.chmod(0o444)
            self.mfs._fsync_directory(destination.parent)
            result[name] = path
        return result

    def _walk(self) -> Iterator[Path]:
        def walk(directory: Path) -> Iterator[Path]:
            with os.scandir(directory) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    if entry.is_dir(follow_symlinks=False):
                        yield from walk(path)
                    yield path

        for directory in ("objects", "artifacts", "staging", "work"):
            yield from walk(self.mfs._path / directory)

    def collect(self) -> GCReport:
        mfs, policy = self.mfs, self.policy
        if not self._collect_lock.acquire(blocking=False):
            return GCReport(busy=True)
        deleted = skipped = 0
        deferred: set[str] = set()
        started = time.monotonic()
        try:
            for _ in range(policy.cycle_files):
                if time.monotonic() - started >= policy.cycle_seconds:
                    break
                if not mfs._condition.acquire(blocking=False):
                    return GCReport(deleted, skipped, busy=True)
                try:
                    if (
                        mfs._stopping
                        or mfs._artifact_readers
                        or mfs._executing
                        or time.monotonic() - mfs._last_activity < policy.idle_seconds
                    ):
                        return GCReport(deleted, skipped, busy=True)
                    catalog = mfs._catalog
                    catalog.connection.execute("PRAGMA busy_timeout=0")
                    try:
                        with catalog.transaction():
                            excluded = (
                                " AND path NOT IN (" + ",".join("?" for _ in deferred) + ")"
                                if deferred
                                else ""
                            )
                            row = catalog.connection.execute(
                                "SELECT path FROM artifacts WHERE (state='deleting' "
                                "OR (state='live' AND unreferenced_at<=?)) "
                                "AND NOT EXISTS(SELECT 1 FROM artifact_refs "
                                "WHERE artifact_refs.path=artifacts.path) "
                                + excluded
                                + " ORDER BY state,path LIMIT 1",
                                (time.time() - policy.grace_seconds, *deferred),
                            ).fetchone()
                            if row:
                                path = str(row[0])
                                catalog.connection.execute(
                                    "UPDATE artifacts SET state='deleting' WHERE path=?", (path,)
                                )
                                catalog.connection.execute(
                                    "DELETE FROM cache WHERE path=?", (path,)
                                )
                    finally:
                        catalog.connection.execute("PRAGMA busy_timeout=30000")
                finally:
                    mfs._condition.release()
                if not row:
                    # Inventory is incremental and never follows links. Discovery gets a
                    # fresh grace period, independent of timestamps on arbitrary old files.
                    if self._inventory is None:
                        self._inventory = self._walk()
                    try:
                        found = next(self._inventory)
                    except StopIteration:
                        self._inventory = None
                        break
                    with catalog.transaction():
                        catalog.register_artifact(found.relative_to(mfs._path).as_posix())
                    continue
                path = str(row[0])
                target = mfs._path / path
                if target.parent not in [
                    mfs._path / d for d in ("objects", "artifacts", "staging", "work")
                ] and not any((mfs._path / d) in target.parents for d in ("staging", "work")):
                    raise CorruptState("GC artifact escaped managed directories")
                try:
                    if target.is_dir() and not target.is_symlink():
                        target.rmdir()  # Non-empty directories wait for their bounded inventory.
                    else:
                        with contextlib.suppress(FileNotFoundError):
                            if not target.is_symlink():
                                target.chmod(0o600, follow_symlinks=False)
                            target.unlink()
                    with catalog.transaction():
                        catalog.connection.execute(
                            "DELETE FROM artifacts WHERE path=? AND state='deleting'", (path,)
                        )
                    deleted += 1
                except OSError:
                    skipped += 1
                    # A non-empty work directory or sharing violation must not
                    # monopolize a zero-grace manual cycle; let inventory advance.
                    deferred.add(path)
                    with catalog.transaction():
                        catalog.connection.execute(
                            "UPDATE artifacts SET state='live',unreferenced_at=? WHERE path=?",
                            (time.time(), path),
                        )
                if (
                    deleted + skipped
                ) % policy.batch_files == 0 and time.monotonic() - started >= policy.batch_seconds:
                    break
            self.last_report = GCReport(deleted, skipped)
        except Exception as error:
            self.last_report = GCReport(deleted, skipped, error=str(error))
        finally:
            self._collect_lock.release()
        return self.last_report

    def maintain(self) -> None:
        mfs = self.mfs
        with mfs._condition:
            while not mfs._stopping:
                mfs._condition.wait_for(lambda: mfs._stopping, self.policy.interval)
                if mfs._stopping:
                    return
                mfs._condition.release()
                try:
                    self.collect()
                finally:
                    mfs._condition.acquire()
