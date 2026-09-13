from __future__ import annotations

import threading
from collections.abc import Generator
from contextlib import contextmanager

from .errors import Closed


class CallGate:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._closing = False
        self._closed = False
        self._active = 0

    @contextmanager
    def call(self) -> Generator[None]:
        with self._condition:
            self.check()
            self._active += 1
        try:
            yield
        finally:
            with self._condition:
                self._active -= 1
                if self._active == 0:
                    self._condition.notify_all()

    def check(self) -> None:
        """Cooperatively stop an admitted call when close starts."""
        with self._condition:
            if self._closing or self._closed:
                raise Closed("MFS instance is closed")

    def start_close(self) -> bool:
        with self._condition:
            if self._closing:
                return False
            self._closing = True
            return True

    def drain(self) -> None:
        with self._condition:
            while self._active:
                self._condition.wait()

    def finish_close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()
