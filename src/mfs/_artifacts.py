# pyright: reportPrivateUsage=false
from __future__ import annotations

import contextlib
import os
import shutil
import sqlite3
import threading
import time
import uuid
from collections.abc import Generator, Iterator, Mapping
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, cast

import blake3

from ._catalog import Catalog
from ._json import canonical_json, compact_json, load_json
from ._lifecycle import Lifecycle
from ._platform import fsync_directory
from ._source import open_regular
from .errors import CorruptState, InvalidQuery, SourceUnavailable, StorageFailed
from .execution import ResourceGrant, ResourceLease
from .types import GCPolicy, GCReport


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
    def __init__(
        self, root: Path, catalog: Catalog, lifecycle: Lifecycle, policy: GCPolicy
    ) -> None:
        self.root, self.catalog, self.lifecycle = root, catalog, lifecycle
        self.policy = policy
        self._collect_lock = threading.Lock()
        self._inventory: Iterator[Path] | None = None
        self.last_report = GCReport()
        self._pins: dict[str, int] = {}
        self._local = threading.local()
        lifecycle.pin_artifact = self.pin

    @contextlib.contextmanager
    def operation(self) -> Generator[None]:
        previous = getattr(self._local, "leases", None)
        leases: list[ResourceLease] = []
        self._local.leases = leases
        try:
            yield
        finally:
            self._local.leases = previous
            for lease in reversed(leases):
                lease.release()

    def protect(self, relative: str) -> None:
        leases: list[ResourceLease] | None = getattr(self._local, "leases", None)
        if leases is not None:
            leases.append(self.pin(relative))

    def pin(self, relative: str) -> ResourceLease:
        with self.lifecycle.condition:
            row = self.catalog.connection.execute(
                "SELECT state FROM artifacts WHERE path=?", (relative,)
            ).fetchone()
            if row and row[0] == "deleting":
                raise SourceUnavailable("artifact is already being retired")
            self._pins[relative] = self._pins.get(relative, 0) + 1

        def release() -> None:
            with self.lifecycle.condition:
                remaining = self._pins[relative] - 1
                if remaining:
                    self._pins[relative] = remaining
                else:
                    del self._pins[relative]
                self.lifecycle.condition.notify_all()

        return ResourceGrant(release)

    def directory(self, incarnation: str, area: str) -> Path:
        if (
            area not in ("originals", "derived", "work")
            or len(incarnation) != 32
            or any(c not in "0123456789abcdef" for c in incarnation)
        ):
            raise CorruptState("invalid namespace storage identity")
        path = self.root
        for part in ("namespaces", incarnation, area):
            path = path / part
            if path.is_symlink():
                raise CorruptState("namespace storage cannot traverse symlinks")
            try:
                path.mkdir()
            except FileExistsError:
                pass
            else:
                fsync_directory(path.parent)
        return path

    def read_text(self, record: dict[str, Any], *, grep: bool = False) -> str:
        reference = record.get("grep_ref") if grep else None
        reference = reference or record.get("text_ref")
        if reference is None:
            if "text" in record:  # Legacy catalog migration, removed after schema upgrade.
                return str(record["text"])
            raise CorruptState("document has no text reference")
        path = self.path(reference["path"]) if reference["owned"] else Path(reference["path"])
        try:
            with open_regular(path) as stream:
                return stream.read().decode(reference.get("encoding", "utf-8"))
        except (OSError, UnicodeError) as error:
            raise SourceUnavailable(f"search text is unavailable: {path}: {error}") from error

    def accept_original(self, staged: Path, incarnation: str, revision: str) -> str:
        destination = self.directory(incarnation, "originals") / revision
        os.replace(staged, destination)
        fsync_directory(destination.parent)
        return destination.relative_to(self.root).as_posix()

    def path(self, relative: str, *, leaf_symlink: bool = False) -> Path:
        parts = PurePosixPath(relative).parts
        if not parts or PurePosixPath(relative).is_absolute() or ".." in parts or "\\" in relative:
            raise CorruptState("invalid managed file reference")
        legacy = parts[0] in ("objects", "artifacts", "work", "staging") and len(parts) >= 2
        namespaced = (
            len(parts) >= 4
            and parts[0] == "namespaces"
            and parts[2] in ("originals", "derived", "work")
        )
        if not legacy and not namespaced:
            raise CorruptState("reference is outside managed file directories")
        path = self.root
        for index, part in enumerate(parts):
            path = path / part
            if path.is_symlink() and not (leaf_symlink and index == len(parts) - 1):
                raise CorruptState("managed file reference traverses a symlink")
        return path

    @staticmethod
    def key(kind: str, value: Any) -> str:
        return kind + ":" + blake3.blake3(canonical_json(value)).hexdigest()

    def cached(self, key: str) -> Any | None:
        with self.lifecycle.condition:
            return self._cached(key)

    def _cached(self, key: str) -> Any | None:
        catalog = self.catalog
        row = catalog.connection.execute(
            "SELECT cache.path,cache.digest FROM cache JOIN artifacts USING(path) "
            "WHERE key=? AND state='live'",
            (key,),
        ).fetchone()
        if row is None:
            return None
        try:
            self.protect(str(row[0]))
            path = self.path(str(row[0]))
            data = path.read_bytes()
            if blake3.blake3(data).hexdigest() != row[1]:
                raise CorruptState("cached artifact checksum mismatch")
            value = load_json(data.decode("utf-8"))
            if isinstance(value, dict):
                for reference in self.catalog.references(value):
                    self.protect(reference)
            return value
        except (CorruptState, SourceUnavailable, OSError, ValueError):
            with catalog.transaction():
                catalog.connection.execute("DELETE FROM cache WHERE key=?", (key,))
            return None

    def cache(self, key: str, path: str) -> None:
        catalog = self.catalog
        digest = blake3.blake3((self.root / path).read_bytes()).hexdigest()
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
            area = work_dir.relative_to(self.root).parts
            directory = self.directory(area[1], "derived")
            path = (directory / (uuid.uuid4().hex + ".bin")).relative_to(self.root).as_posix()
            self.protect(path)
            with self.catalog.transaction():
                self.catalog.register_artifact(path)
            destination = self.root / path
            with candidate.open("rb") as source, destination.open("xb") as target:
                shutil.copyfileobj(source, target, 1024 * 1024)
                target.flush()
                os.fsync(target.fileno())
            # Read-only is a contract as well as a useful guard against accidental edits.
            destination.chmod(0o444)
            fsync_directory(destination.parent)
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
            yield from walk(self.root / directory)
        for namespace in (self.root / "namespaces").iterdir():
            if namespace.is_symlink() or not namespace.is_dir():
                continue
            for area in ("originals", "derived", "work"):
                directory = namespace / area
                if directory.is_dir() and not directory.is_symlink():
                    yield from walk(directory)

    def collect(self) -> GCReport:
        lifecycle, policy = self.lifecycle, self.policy
        if not self._collect_lock.acquire(blocking=False):
            return GCReport(busy=True)
        deleted = skipped = 0
        deferred: set[str] = set()
        started = time.monotonic()
        try:
            for _ in range(policy.cycle_files):
                if time.monotonic() - started >= policy.cycle_seconds:
                    break
                if not lifecycle.condition.acquire(blocking=False):
                    return GCReport(deleted, skipped, busy=True)
                try:
                    if (
                        lifecycle.stopping
                        or lifecycle.boot_paused
                        or time.monotonic() - lifecycle.last_activity < policy.idle_seconds
                    ):
                        return GCReport(deleted, skipped, busy=True)
                    catalog = self.catalog
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
                                if any(path == p or path.startswith(p + "/") for p in self._pins):
                                    deferred.add(path)
                                    continue
                                catalog.connection.execute(
                                    "UPDATE artifacts SET state='deleting' WHERE path=?", (path,)
                                )
                                catalog.connection.execute(
                                    "DELETE FROM cache WHERE path=?", (path,)
                                )
                    finally:
                        catalog.connection.execute("PRAGMA busy_timeout=30000")
                finally:
                    lifecycle.condition.release()
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
                        catalog.register_artifact(found.relative_to(self.root).as_posix())
                    continue
                path = str(row[0])
                target = self.path(path, leaf_symlink=True)
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
            cause: BaseException | None = error
            busy = False
            while cause is not None:
                if isinstance(cause, sqlite3.Error) and (
                    getattr(cause, "sqlite_errorcode", 0) & 0xFF
                ) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    busy = True
                    break
                cause = cause.__cause__
            self.last_report = GCReport(
                deleted, skipped, busy=busy, error=None if busy else str(error)
            )
        finally:
            self._collect_lock.release()
        return self.last_report

    def maintain(self) -> None:
        lifecycle = self.lifecycle
        with lifecycle.condition:
            while not lifecycle.stopping:
                lifecycle.condition.wait_for(lambda: lifecycle.stopping, self.policy.interval)
                if lifecycle.stopping:
                    return
                lifecycle.condition.release()
                try:
                    self.collect()
                finally:
                    lifecycle.condition.acquire()

    def write(self, name: str, value: Any, incarnation: str) -> str:
        relative = (
            (self.directory(incarnation, "derived") / (name + "-" + uuid.uuid4().hex + ".json"))
            .relative_to(self.root)
            .as_posix()
        )
        self.protect(relative)
        with self.catalog.transaction():
            self.catalog.register_artifact(relative)
        self.write_json(self.root / relative, value)
        return relative

    def write_json(self, path: Path, value: Any) -> None:
        temporary = path.parent / ("." + uuid.uuid4().hex + ".tmp")
        self.protect(temporary.relative_to(self.root).as_posix())
        try:
            with temporary.open("x", encoding="utf-8") as stream:
                stream.write(compact_json(value))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            fsync_directory(path.parent)
        except OSError as error:
            raise StorageFailed(f"failed to persist {path.name}: {error}") from error
        finally:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()

    def read(self, relative: str) -> Any:
        path = self.path(relative)
        try:
            return load_json(path.read_text("utf-8"))
        except (OSError, ValueError) as error:
            raise CorruptState(f"cannot read artifact: {error}") from error
