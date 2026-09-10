# pyright: reportPrivateUsage=false
from __future__ import annotations

import os
import stat
import unicodedata
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

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

if TYPE_CHECKING:
    from ._core import MFS


def _same(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _case_sensitive(root: Path, descriptor: int) -> bool:
    # Observe actual filesystem lookup, rather than lowercasing paths on every POSIX volume.
    with os.scandir(descriptor) as iterator:
        names = [e.name for e in iterator]
    for name in names:
        swapped = name.swapcase()
        if swapped == name:
            continue
        try:
            return not _same(
                os.stat(name, dir_fd=descriptor, follow_symlinks=False),
                os.stat(swapped, dir_fd=descriptor, follow_symlinks=False),
            )
        except FileNotFoundError:
            return True
    if root.name.swapcase() != root.name and not os.path.ismount(root):
        try:
            return not os.path.samefile(root, root.with_name(root.name.swapcase()))
        except FileNotFoundError:
            return True
    return True


def _open_relative(root_fd: int, relative: str) -> tuple[int, str]:
    current = os.dup(root_fd)
    actual: list[str] = []
    try:
        if relative == ".":
            return current, "."
        parts = relative.split("/")
        for index, name in enumerate(parts):
            with os.scandir(current) as iterator:
                entries = list(iterator)
            chosen = next((e.name for e in entries if e.name == name), None)
            if chosen is None:
                try:
                    requested_stat = os.stat(name, dir_fd=current, follow_symlinks=False)
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
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            if index < len(parts) - 1:
                flags |= os.O_DIRECTORY
            following = os.open(chosen, flags, dir_fd=current)
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
    info = mfs._required_namespace(namespace)
    if info.kind != "external" or info.root is None:
        raise WrongNamespaceKind("sync requires an external namespace")
    report_path = requested
    changed: set[DocumentId] = set()
    removed: set[DocumentId] = set()
    failures: dict[tuple[str, str], SyncFailure] = {}
    skipped: dict[tuple[str, str], SyncSkipped] = {}
    seen: set[str] = set()
    protected: set[str] = set()
    nonmembers: set[str] = set()
    complete = True
    root_fd: int | None = None

    def fail(relative: str, error: Exception) -> None:
        nonlocal complete
        complete = False
        code = error.code if isinstance(error, MFSError) else "SourceUnavailable"
        failures[(relative, code)] = SyncFailure(relative, code, str(error))

    def skip(relative: str, reason: str) -> None:
        skipped[(relative, reason)] = SyncSkipped(relative, cast(Any, reason))

    def result() -> SyncReport:
        return SyncReport(
            namespace,
            report_path,
            complete,
            tuple(sorted(changed)),
            tuple(sorted(removed)),
            tuple(failures[k] for k in sorted(failures)),
            tuple(skipped[k] for k in sorted(skipped)),
            not mfs._pending and mfs._state == "ready",
        )

    try:
        root = info.root.resolve(strict=True)
        if root == mfs._path or root in mfs._path.parents or mfs._path in root.parents:
            raise RootOverlap("external root and mfs_path overlap")
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
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

        with mfs._condition:
            ns = dict(mfs._namespaces[namespace])
            if ns.get("root_actual") != str(root):
                ns.update(root_actual=str(root), binding=uuid.uuid4().hex)
                updates: list[tuple[DocumentId, dict[str, Any]]] = []
                with mfs._catalog.transaction():
                    mfs._catalog.put_namespace(namespace, ns)
                    for identity, previous in mfs._targets.items():
                        if identity.namespace == namespace and previous["kind"] == "upsert":
                            target = dict(
                                previous,
                                state="cancelled",
                                error="root binding changed; awaiting reconciliation",
                            )
                            mfs._catalog.put_target(namespace, identity.doc_id, target)
                            updates.append((identity, target))
                mfs._namespaces[namespace] = ns
                for identity, target in updates:
                    mfs._remember(identity, target)
                requested = "."  # A new root target changes membership for the whole namespace.

        def file(descriptor: int, relative: str, *, exact: bool) -> None:
            seen.add(relative)
            protected.add(relative)
            identity = DocumentId(namespace, relative)
            metadata = os.fstat(descriptor)
            if mfs._excluded(relative):
                nonmembers.add(relative)
                skip(relative, "excluded")
                return
            maximum = mfs._sync_policy.max_file_bytes
            if maximum is not None and metadata.st_size > maximum:
                nonmembers.add(relative)
                skip(relative, "too_large")
                return
            previous = mfs._targets.get(identity)
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
                    (p for p in mfs._processors if suffix in mfs._processor_suffixes[id(p)]), None
                )
                if (
                    processor is not None
                    and previous.get("binding") == ns["binding"]
                    and source.get("size") == metadata.st_size
                    and source.get("mtime_ns") == metadata.st_mtime_ns
                    and previous.get("processor") == mfs._processor_descriptions[id(processor)]
                ):
                    return
            staged = None
            try:
                staged = mfs._stage_descriptor(descriptor)
                if not root_stable():
                    raise SourceChanged("root changed during observation")
                # Reopen the canonical relative identity without following parent symlinks.
                check, _ = _open_relative(root_fd, relative)
                try:
                    if not _same(os.fstat(check), os.fstat(descriptor)):
                        raise SourceChanged("file identity changed during observation")
                finally:
                    os.close(check)
                report = mfs._admit(identity, staged, force=force)
                if report.outcome != "unchanged":
                    changed.add(identity)
            except UnsupportedMediaType:
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
                if not real.is_file() or mfs._excluded(relative) or mfs._excluded(canonical):
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

        def walk(descriptor: int, relative_dir: str) -> None:
            try:
                with os.scandir(descriptor) as iterator:
                    entries = sorted(iterator, key=lambda e: e.name.encode())
                for entry in entries:
                    relative = (
                        entry.name if relative_dir == "." else relative_dir + "/" + entry.name
                    )
                    if mfs._excluded(relative):
                        nonmembers.add(relative)
                        skip(relative, "excluded")
                        continue
                    try:
                        metadata = entry.stat(follow_symlinks=False)
                        if stat.S_ISLNK(metadata.st_mode):
                            link(relative)
                            continue
                        if not stat.S_ISREG(metadata.st_mode) and not stat.S_ISDIR(
                            metadata.st_mode
                        ):
                            nonmembers.add(relative)
                            skip(relative, "special_file")
                            continue
                        child = os.open(
                            entry.name,
                            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
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
            if requested != "." and mfs._excluded(requested):
                nonmembers.add(requested)
                skip(requested, "excluded")
            elif requested != "." and candidate.is_symlink():
                link(requested)
            else:
                descriptor, actual_requested = _open_relative(root_fd, requested)
                try:
                    metadata = os.fstat(descriptor)
                    if requested != "." and mfs._excluded(actual_requested):
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
        except OSError as error:
            fail(requested, error)
        if not root_stable():
            fail(".", SourceChanged("root changed during observation"))
        with mfs._condition:
            existing = [
                i
                for i, j in mfs._targets.items()
                if i.namespace == namespace and j["kind"] == "upsert"
            ]
        seen_keys = {key(s) for s in seen}
        for identity in existing:
            relative = identity.doc_id
            excluded = any(under(relative, prefix) for prefix in nonmembers)
            absent = (
                complete and under(relative, actual_requested) and key(relative) not in seen_keys
            )
            replacement_child = any(
                key(relative) != key(prefix) and under(relative, prefix) for prefix in protected
            )
            if (excluded or (absent and not replacement_child)) and (
                mfs._remove(identity).outcome == "removed"
            ):
                removed.add(identity)
    except (OSError, RuntimeError, MFSError) as error:
        fail(".", error)
    finally:
        if root_fd is not None:
            os.close(root_fd)
    return result()
