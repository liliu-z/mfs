# pyright: reportPrivateUsage=false
from __future__ import annotations

import os
import stat
import unicodedata
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from ._platform import validate_windows_relative, windows_files
from ._validation import suffix_for, validate_external_path
from .errors import (
    InvalidPath,
    MFSError,
    RootOverlap,
    SourceChanged,
    UnsupportedMediaType,
    WrongNamespaceKind,
)
from .types import DocumentId, SyncFailure, SyncReport, SyncSkipped

files = windows_files() if os.name == "nt" else os
NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
NONBLOCK = getattr(os, "O_NONBLOCK", 0)
DIRECTORY = getattr(os, "O_DIRECTORY", 0)

if TYPE_CHECKING:
    from ._core import MFS


def _same(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _case_sensitive(root: Path, descriptor: int) -> bool:
    # Observe actual filesystem lookup, rather than lowercasing paths on every POSIX volume.
    with files.scandir(descriptor) as iterator:
        names = [e.name for e in iterator]
    for name in names:
        swapped = name.swapcase()
        if swapped == name:
            continue
        try:
            return not _same(
                files.stat(name, dir_fd=descriptor, follow_symlinks=False),
                files.stat(swapped, dir_fd=descriptor, follow_symlinks=False),
            )
        except FileNotFoundError:
            return True
    if root.name.swapcase() != root.name and not os.path.ismount(root):
        try:
            return not os.path.samefile(root, root.with_name(root.name.swapcase()))
        except FileNotFoundError:
            return True
    return True


def _open_canonical(root_fd: int, relative: str) -> int:
    """Reopen a scanner's known spelling without enumerating parent directories."""
    current = os.dup(root_fd)
    try:
        parts = relative.split("/")
        for index, name in enumerate(parts):
            flags = os.O_RDONLY | NOFOLLOW | NONBLOCK
            if index < len(parts) - 1:
                flags |= DIRECTORY
            following = files.open(name, flags, dir_fd=current)
            os.close(current)
            current = following
        return current
    except BaseException:
        os.close(current)
        raise


def _open_relative(root_fd: int, relative: str) -> tuple[int, str]:
    current = os.dup(root_fd)
    actual: list[str] = []
    try:
        if relative == ".":
            return current, "."
        parts = relative.split("/")
        for index, name in enumerate(parts):
            with files.scandir(current) as iterator:
                entries = list(iterator)
            chosen = next((e.name for e in entries if e.name == name), None)
            if chosen is None:
                try:
                    requested_stat = files.stat(name, dir_fd=current, follow_symlinks=False)
                    chosen = next(
                        (
                            e.name
                            for e in entries
                            if _same(e.stat(follow_symlinks=False), requested_stat)
                        ),
                        None,
                    )
                except FileNotFoundError:
                    raise FileNotFoundError("/".join([*actual, *parts[index:]])) from None
            if chosen is None:
                raise FileNotFoundError("/".join([*actual, *parts[index:]]))
            flags = os.O_RDONLY | NOFOLLOW | NONBLOCK
            if index < len(parts) - 1:
                flags |= DIRECTORY
            following = files.open(chosen, flags, dir_fd=current)
            os.close(current)
            current = following
            actual.append(chosen)
        return current, "/".join(actual)
    except Exception:
        os.close(current)
        raise


def sync_namespace(mfs: MFS, namespace: str, path: str, *, verify: str, force: bool) -> SyncReport:
    if verify not in ("stat", "content"):
        raise InvalidPath("verify must be stat or content")
    requested = validate_external_path(path, allow_root=True)
    if os.name == "nt":
        validate_windows_relative(requested)
    with mfs._condition:
        info = mfs._required_namespace(namespace)
        initial = dict(mfs._tasks.namespaces[namespace])
    if info.kind != "external" or info.root is None:
        raise WrongNamespaceKind("sync requires an external namespace")
    mfs._require_modern_namespace(namespace)
    report_path = requested
    changed: set[DocumentId] = set()
    removed: set[DocumentId] = set()
    failures: dict[tuple[str, str], SyncFailure] = {}
    skipped: dict[tuple[str, str], SyncSkipped] = {}
    seen: set[str] = set()
    protected: set[str] = set()
    nonmembers: set[str] = set()
    observed_directories: set[str] = set()
    uncertain: set[str] = set()
    complete = True
    root_fd: int | None = None

    def fail(relative: str, error: Exception) -> None:
        nonlocal complete
        complete = False
        uncertain.add(relative)
        code = error.code if isinstance(error, MFSError) else "SourceUnavailable"
        failures[(relative, code)] = SyncFailure(relative, code, str(error))

    def skip(relative: str, reason: str) -> None:
        skipped[(relative, reason)] = SyncSkipped(relative, cast(Any, reason))

    def result() -> SyncReport:
        removed.difference_update(changed)
        return SyncReport(
            namespace,
            report_path,
            complete,
            tuple(sorted(changed)),
            tuple(sorted(removed)),
            tuple(failures[k] for k in sorted(failures)),
            tuple(skipped[k] for k in sorted(skipped)),
            mfs._is_ready(),
        )

    try:
        root = info.root.resolve(strict=True)
        if root == mfs._path or root in mfs._path.parents or mfs._path in root.parents:
            raise RootOverlap("external root and mfs_path overlap")
        root_fd = files.open(root, os.O_RDONLY | DIRECTORY | NOFOLLOW)
        root_identity = os.fstat(root_fd)
        case_sensitive = _case_sensitive(root, root_fd)

        def key(value: str) -> str:
            return value if case_sensitive else unicodedata.normalize("NFD", value).casefold()

        def under(value: str, prefix: str) -> bool:
            value, prefix = key(value), key(prefix)
            return prefix == "." or value == prefix or value.startswith(prefix + "/")

        def root_stable() -> bool:
            try:
                return (
                    info.root is not None
                    and info.root.resolve(strict=True) == root
                    and _same(root.stat(), root_identity)
                )
            except OSError:
                return False

        if not root_stable():
            raise SourceChanged("root changed before observation")
        with mfs._condition:
            ns = dict(mfs._tasks.namespaces.get(namespace, {}))
            if ns.get("incarnation") != initial["incarnation"] or ns.get("root") != initial["root"]:
                raise SourceChanged("namespace was replaced while opening its root")
            if ns.get("root_actual") != str(root):
                removed.update(mfs._tasks.retarget_root(namespace, str(root)))
                requested = "."  # A new root target changes membership for the whole namespace.
                ns = dict(mfs._tasks.namespaces[namespace])
            baseline = {
                doc: revision
                for doc, revision in mfs._catalog.query(
                    "SELECT doc_id,revision FROM targets WHERE namespace=?", (namespace,)
                )
            }

        def validate_observation() -> None:
            current = mfs._tasks.namespaces.get(namespace, {})
            if current.get("building", {}).get("generation") != ns.get("building", {}).get(
                "generation"
            ) or any(
                current.get(k) != ns.get(k)
                for k in ("incarnation", "binding", "rules_revision", "root", "root_actual")
            ):
                raise SourceChanged(
                    "namespace configuration changed during observation; sync again"
                )

        def file(descriptor: int, relative: str, *, exact: bool) -> None:
            if os.name == "nt" and not files.path(descriptor).is_relative_to(root):
                fail(relative, SourceChanged("opened file moved outside the root"))
                return
            seen.add(relative)
            protected.add(relative)
            identity = DocumentId(namespace, relative)
            metadata = os.fstat(descriptor)
            if mfs._excluded(namespace, relative):
                nonmembers.add(relative)
                skip(relative, "excluded")
                return
            maximum = ns.get("max_file_bytes")
            if maximum is not None and metadata.st_size > maximum:
                nonmembers.add(relative)
                skip(relative, "too_large")
                return
            previous = mfs._tasks.targets.get(identity)
            if (
                previous
                and previous["kind"] == "upsert"
                and verify == "stat"
                and not exact
                and not force
            ):
                source = previous.get("source", {})
                suffix = suffix_for(relative)
                processor = next(
                    (p for p in ns["manifest"]["processors"] if suffix in p["suffix_media_types"]),
                    None,
                )
                if (
                    processor is not None
                    and previous.get("binding") == ns["binding"]
                    and source.get("size") == metadata.st_size
                    and source.get("mtime_ns") == metadata.st_mtime_ns
                    and previous.get("processor")
                    == {k: processor[k] for k in ("id", "version", "options")}
                ):
                    return
            staged = None
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                selection = mfs._select_processor(
                    namespace,
                    relative,
                    root / relative,
                    None,
                    None,
                    head=os.read(descriptor, 64 * 1024),
                )
                staged = mfs._stage_descriptor(descriptor, root / relative)
                if not root_stable():
                    raise SourceChanged("root changed during observation")
                # Reopen the canonical relative identity without following parent symlinks.
                check = _open_canonical(root_fd, relative)
                try:
                    if not _same(os.fstat(check), os.fstat(descriptor)):
                        raise SourceChanged("file identity changed during observation")
                finally:
                    os.close(check)
                with mfs._condition:
                    validate_observation()
                    report = mfs._admit(identity, staged, selection=selection, force=force)
                if report.outcome != "unchanged":
                    changed.add(identity)
            except UnsupportedMediaType:
                nonmembers.add(relative)
                skip(relative, "unsupported_media_type")
            except Exception as error:
                fail(relative, error)
            finally:
                if staged is not None:
                    mfs._remove_staging(staged.directory)

        def link(relative: str) -> None:
            nonmembers.add(relative)
            try:
                real = (root / relative).resolve(strict=True)
                canonical = real.relative_to(root).as_posix()
                if (
                    not real.is_file()
                    or mfs._excluded(namespace, relative)
                    or mfs._excluded(namespace, canonical)
                ):
                    skip(relative, "symlink")
                    return
                if canonical in seen:
                    return
                descriptor, actual = _open_relative(root_fd, canonical)
                try:
                    if stat.S_ISREG(os.fstat(descriptor).st_mode):
                        file(descriptor, actual, exact=True)
                finally:
                    os.close(descriptor)
            except (OSError, ValueError, RuntimeError):
                skip(relative, "symlink")

        may_reinclude = any(rule["action"] == "include" for rule in ns["rules"])

        def prune(relative: str, directory: bool) -> bool:
            return (not directory or not may_reinclude) and mfs._excluded(
                namespace, relative, directory=directory
            )

        def walk(descriptor: int, relative_dir: str) -> None:
            try:
                if os.name == "nt" and not files.path(descriptor).is_relative_to(root):
                    raise SourceChanged("opened directory moved outside the root")
                with files.scandir(descriptor) as iterator:
                    entries = sorted(iterator, key=lambda e: e.name.encode())
                observed_directories.add(relative_dir)
                for entry in entries:
                    relative = (
                        entry.name if relative_dir == "." else relative_dir + "/" + entry.name
                    )
                    try:
                        metadata = entry.stat(follow_symlinks=False)
                        if prune(relative, stat.S_ISDIR(metadata.st_mode)):
                            nonmembers.add(relative)
                            skip(relative, "excluded")
                            continue
                        if (
                            stat.S_ISLNK(metadata.st_mode)
                            or getattr(metadata, "st_file_attributes", 0) & 0x400
                        ):
                            link(relative)
                            continue
                        if not stat.S_ISREG(metadata.st_mode) and not stat.S_ISDIR(
                            metadata.st_mode
                        ):
                            nonmembers.add(relative)
                            skip(relative, "special_file")
                            continue
                        child = files.open(
                            entry.name,
                            os.O_RDONLY | NOFOLLOW | NONBLOCK,
                            dir_fd=descriptor,
                        )
                        try:
                            actual = os.fstat(child)
                            if not _same(actual, metadata):
                                raise SourceChanged("entry changed while opening")
                            if stat.S_ISDIR(actual.st_mode):
                                walk(child, relative)
                            else:
                                file(child, relative, exact=False)
                        finally:
                            os.close(child)
                    except OSError as error:
                        fail(relative, error)
            except OSError as error:
                fail(relative_dir, error)

        actual_requested = requested
        try:
            # A final file alias is permitted; parent directory aliases are not traversed.
            candidate = root / requested
            if requested != "." and prune(requested, candidate.is_dir()):
                nonmembers.add(requested)
                skip(requested, "excluded")
            elif requested != "." and candidate.is_symlink():
                link(requested)
            else:
                descriptor, actual_requested = _open_relative(root_fd, requested)
                try:
                    metadata = os.fstat(descriptor)
                    if requested != "." and prune(actual_requested, stat.S_ISDIR(metadata.st_mode)):
                        nonmembers.add(actual_requested)
                        skip(actual_requested, "excluded")
                    elif stat.S_ISDIR(metadata.st_mode):
                        walk(descriptor, actual_requested)
                    elif stat.S_ISREG(metadata.st_mode):
                        file(descriptor, actual_requested, exact=True)
                    else:
                        nonmembers.add(actual_requested)
                        skip(actual_requested, "special_file")
                finally:
                    os.close(descriptor)
        except FileNotFoundError as error:
            actual_requested = str(error)
            # The exact requested identity (possibly an entire subtree) is absent.
            nonmembers.add(actual_requested)
        except OSError as error:
            fail(requested, error)
        if not root_stable():
            fail(".", SourceChanged("root changed during observation"))
        with mfs._condition:
            validate_observation()
            existing = [
                i
                for i, j in mfs._tasks.targets.items()
                if i.namespace == namespace and j["kind"] == "upsert"
            ]
        seen_keys = {key(s) for s in seen}
        observed_keys = {key(s) for s in observed_directories}
        uncertain_keys = {key(s) for s in uncertain}
        nonmember_keys = {key(s) for s in nonmembers}
        protected_keys = {key(s) for s in protected}
        for identity in existing:
            relative = identity.doc_id
            # Check path ancestors instead of scanning every observed directory
            # for every file in a large tree.
            parts = key(relative).split("/")
            ancestors = {".", *("/".join(parts[:end]) for end in range(1, len(parts) + 1))}
            excluded = bool(ancestors & nonmember_keys)
            absent = (
                under(relative, actual_requested)
                and key(relative) not in seen_keys
                and bool(ancestors & observed_keys)
                and not ancestors & uncertain_keys
            )
            replacement_child = bool((ancestors - {key(relative)}) & protected_keys)
            spelling_changed = (
                not case_sensitive and key(relative) in seen_keys and relative not in seen
            )
            if spelling_changed or excluded or absent or replacement_child:
                with mfs._condition:
                    validate_observation()
                    current = mfs._tasks.targets.get(identity, {})
                    if current.get("revision") != baseline.get(identity.doc_id):
                        continue
                    if mfs._tasks.remove(identity).outcome == "removed":
                        removed.add(identity)
    except (OSError, RuntimeError, MFSError) as error:
        fail(".", error)
    finally:
        if root_fd is not None:
            os.close(root_fd)
    return result()
