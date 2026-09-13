from __future__ import annotations

import threading
from collections.abc import Generator
from contextlib import contextmanager


class CallGate:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._closing = False
        self._closed = False
        self._active = 0

    @contextmanager
    def call(self) -> Generator[None]:
        from .errors import Closed

        with self._condition:
            if self._closing or self._closed:
                raise Closed("MFS instance is closed")
            self._active += 1
        try:
            yield
        finally:
            with self._condition:
                self._active -= 1
                if self._active == 0:
                    self._condition.notify_all()

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
