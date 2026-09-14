from __future__ import annotations

import threading
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Protocol

from .errors import Closed, ExecutionTimeout


class Deadline(Protocol):
    def check(self) -> None: ...
    def remaining(self) -> float | None: ...


class MaintenanceDeadline:
    def __init__(self, timeout: float, stopping: Callable[[], bool], expired: Callable[[], None]):
        self.until = time.monotonic() + timeout
        self.stopping = stopping
        self.expired = expired
        self.reported = False
        self.committed = False

    def check(self) -> None:
        if time.monotonic() >= self.until:
            raise ExecutionTimeout("index maintenance exceeded its stage timeout")
        if self.stopping():
            raise Closed("MFS instance is closing")

    def remaining(self) -> float:
        self.check()
        return max(0.0, self.until - time.monotonic())


class BackendDeadlines:
    """Supervise management calls without treating timeout as actual retirement."""

    def __init__(
        self, condition: threading.Condition, timeout: float, stopping: Callable[[], bool]
    ) -> None:
        self.condition, self.timeout, self.stopping = condition, timeout, stopping
        self.active: set[MaintenanceDeadline] = set()

    def expire(self) -> None:
        with self.condition:
            changed = False
            for deadline in tuple(self.active):
                if (
                    not deadline.reported
                    and not deadline.committed
                    and time.monotonic() >= deadline.until
                ):
                    deadline.expired()
                    deadline.reported = True
                    changed = True
            if changed:
                self.condition.notify_all()

    @contextmanager
    def operation(self, expired: Callable[[], None]) -> Generator[MaintenanceDeadline]:
        deadline = MaintenanceDeadline(self.timeout, self.stopping, expired)
        with self.condition:
            self.active.add(deadline)
        try:
            deadline.check()
            yield deadline
            if not deadline.committed:
                deadline.check()
        finally:
            with self.condition:
                try:
                    self.expire()
                finally:
                    self.active.discard(deadline)
