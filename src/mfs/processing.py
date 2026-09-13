from __future__ import annotations

import math
import os
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

from ._json import JSONValue, copy_json
from .types import DocumentId


def run_process_supervisor() -> None:
    """Call before app initialization in a frozen executable that uses run_process.

    Normal invocations return immediately. MFS's private supervisor invocation
    retires its command and exits without starting the application's daemon.
    """
    if len(sys.argv) > 1 and sys.argv[1] == "--mfs-process-supervisor":
        from ._process_supervisor import main

        raise SystemExit(main(sys.argv[2:]))


class _ProcessingStopped(BaseException):
    pass


class _ProcessingYielded(BaseException):
    pass


class Cancellation:
    """Cooperative cancellation; managed subprocesses are also retired by MFS."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._reason: str | None = None
        self._yield_requested = False
        self._started_at = time.monotonic()

    @property
    def reason(self) -> str | None:
        return self._reason

    def _cancel(self, reason: str) -> None:
        self._reason = reason
        self._event.set()

    def check(self) -> None:
        if self._event.is_set():
            raise _ProcessingStopped(self._reason)
        if self._yield_requested:
            raise _ProcessingYielded()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)


class ProcessingContext:
    """Attempt-scoped input, durable resume data and controlled execution.

    Use paths in work_dir for outputs. Checkpoint files are immutable copies;
    copy them into work_dir before editing. A checkpoint may return worker
    capacity by raising an internal yield; do not catch BaseException around it.
    """

    def __init__(
        self,
        document_id: DocumentId,
        revision: str,
        content_hash: str,
        work_dir: Path,
        cancellation: Cancellation,
        resume_state: JSONValue,
        resume_files: Mapping[str, Path],
        checkpoint: Callable[[JSONValue, Mapping[str, Path]], bool],
        progress: Callable[[float, float | None, str | None], None],
        *,
        process_owner: int | None = None,
    ) -> None:
        self.document_id = document_id
        self.revision = revision
        self.content_hash = content_hash
        self.work_dir = work_dir
        self.cancellation = cancellation
        self.resume_state = copy_json(resume_state)
        self.resume_files = MappingProxyType(dict(resume_files))
        self._checkpoint = checkpoint
        self._progress = progress
        self._process_owner = process_owner

    def report_progress(
        self, completed: float, total: float | None = None, unit: str | None = None
    ) -> None:
        self.cancellation.check()
        if (
            not math.isfinite(completed)
            or completed < 0
            or (total is not None and (not math.isfinite(total) or total < completed))
        ):
            raise ValueError("progress must be finite, non-negative and no greater than total")
        self._progress(completed, total, unit)

    def checkpoint(self, state: JSONValue, *, files: Mapping[str, Path] | None = None) -> None:
        self.cancellation.check()
        if self._checkpoint(copy_json(state), files or {}):
            raise _ProcessingYielded()
        self.cancellation.check()

    def run_process(
        self,
        argv: Sequence[str],
        *,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        """Run without a shell, bounded by this attempt's lifetime.

        stdout/stderr are spooled to disk to avoid pipe deadlocks. The adapter
        owns argument and output size policy; every descendant is retired before
        the method returns, including children left by a successful parent.
        """
        self.cancellation.check()
        if not argv or (timeout is not None and (not math.isfinite(timeout) or timeout < 0)):
            raise ValueError("argv must be non-empty and timeout finite and non-negative")
        from ._platform import ProcessTree

        with (
            tempfile.TemporaryFile(dir=self.work_dir) as stdout,
            tempfile.TemporaryFile(dir=self.work_dir) as stderr,
        ):
            watch_read = watch_write = None
            command = list(argv)
            options: dict[str, Any] = {}
            if os.name == "posix":
                watch_read, watch_write = os.pipe()
                inherited = [watch_read]
                if self._process_owner is not None:
                    inherited.append(self._process_owner)
                options.update(start_new_session=True, pass_fds=tuple(inherited))
                supervisor = (
                    "--mfs-process-supervisor"
                    if getattr(sys, "frozen", False)
                    else str(Path(__file__).with_name("_process_supervisor.py"))
                )
                command = [sys.executable, supervisor, str(watch_read), *command]
            else:
                options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | 0x4
            try:
                process = subprocess.Popen(
                    command,
                    cwd=self.work_dir,
                    env=env,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    **options,
                )
            except BaseException:
                if os.name == "posix":
                    assert watch_write is not None
                    os.close(watch_write)
                raise
            finally:
                if os.name == "posix":
                    assert watch_read is not None
                    os.close(watch_read)
            tree = None
            started = time.monotonic()
            try:
                if os.name != "posix":
                    tree = ProcessTree(cast(subprocess.Popen[bytes], process))
                while process.poll() is None:
                    self.cancellation.check()
                    if timeout is not None and time.monotonic() - started >= timeout:
                        raise subprocess.TimeoutExpired(list(argv), timeout)
                    self.cancellation.wait(0.025)
                self.cancellation.check()
                if tree is not None:
                    tree.close()
                    tree = None
                stdout.seek(0)
                stderr.seek(0)
                result = subprocess.CompletedProcess(
                    list(argv), process.returncode, stdout.read(), stderr.read()
                )
                result.check_returncode()
                return result
            finally:
                if os.name == "posix":
                    assert watch_write is not None
                    os.close(watch_write)  # Supervisor retires the entire command group.
                elif tree is not None:
                    tree.close()
                elif process.poll() is None:
                    process.kill()
                process.wait()
