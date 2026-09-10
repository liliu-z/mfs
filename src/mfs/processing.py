from __future__ import annotations

import math
import os
import subprocess
import tempfile
import threading
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

from ._json import JSONValue, copy_json
from .types import DocumentId


class _ProcessingStopped(BaseException):
    pass


class _ProcessingYielded(BaseException):
    pass


class Cancellation:
    """Cooperative cancellation; managed subprocesses are also retired by MFS."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._reason: str | None = None

    @property
    def reason(self) -> str | None:
        return self._reason

    def _cancel(self, reason: str) -> None:
        self._reason = reason
        self._event.set()

    def check(self) -> None:
        if self._event.is_set():
            raise _ProcessingStopped(self._reason)

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
        import time

        from ._platform import ProcessTree

        with (
            tempfile.TemporaryFile(dir=self.work_dir) as stdout,
            tempfile.TemporaryFile(dir=self.work_dir) as stderr,
        ):
            options: dict[str, Any] = (
                {"start_new_session": True}
                if os.name == "posix"
                else {"creationflags": (subprocess.CREATE_NEW_PROCESS_GROUP | 0x4)}
            )
            process = subprocess.Popen(
                list(argv),
                cwd=self.work_dir,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                **options,
            )
            tree = None
            started = time.monotonic()
            try:
                tree = ProcessTree(cast(subprocess.Popen[bytes], process))
                while process.poll() is None:
                    self.cancellation.check()
                    if timeout is not None and time.monotonic() - started >= timeout:
                        raise subprocess.TimeoutExpired(list(argv), timeout)
                    self.cancellation.wait(0.025)
                self.cancellation.check()
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
                if tree is not None:
                    tree.close()
                elif process.poll() is None:
                    process.kill()
                process.wait()
