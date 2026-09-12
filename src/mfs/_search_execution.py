from __future__ import annotations

import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from .errors import Closed, WaitTimeout


class SearchDeadline:
    """One caller deadline shared by admission, waiting and every search stage."""

    def __init__(self, timeout: float | None, closing: threading.Event) -> None:
        self._until = None if timeout is None else time.monotonic() + timeout
        self._closing = closing

    def check(self) -> None:
        if self._closing.is_set():
            raise Closed("MFS instance is closing")
        if self._until is not None and time.monotonic() >= self._until:
            raise WaitTimeout("search exceeded its timeout")

    def remaining(self) -> float | None:
        self.check()
        return None if self._until is None else max(0.0, self._until - time.monotonic())


class SearchExecution:
    """Bound query concurrency while allowing a caller to stop waiting on slow adapters.

    A timed-out call keeps its slot until the adapter returns. No unbounded
    executor queue or new thread is created by repeatedly timing out a query.
    Search bodies own their MFS resource lease until they actually finish.
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._closing = threading.Event()
        self._capacity = 4
        self._active = 0
        self._executor = ThreadPoolExecutor(
            max_workers=self._capacity, thread_name_prefix="mfs-search"
        )

    def run[T](self, timeout: float | None, search: Callable[[SearchDeadline], T]) -> T:
        deadline = SearchDeadline(timeout, self._closing)
        with self._condition:
            self._condition.wait_for(
                lambda: self._closing.is_set() or self._active < self._capacity,
                deadline.remaining(),
            )
            deadline.check()
            self._active += 1

        def execute() -> T:
            try:
                deadline.check()
                result = search(deadline)
                deadline.check()
                return result
            finally:
                with self._condition:
                    self._active -= 1
                    self._condition.notify_all()

        try:
            future = self._executor.submit(execute)
        except BaseException:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()
            raise

        def notify_completion(_: object) -> None:
            with self._condition:
                self._condition.notify_all()

        future.add_done_callback(notify_completion)
        with self._condition:
            self._condition.wait_for(
                lambda: self._closing.is_set() or future.done(), deadline.remaining()
            )
        deadline.check()
        return future.result()

    def stop(self) -> None:
        self._closing.set()
        with self._condition:
            self._condition.notify_all()

    def close(self) -> None:
        self.stop()
        self._executor.shutdown(wait=True)
