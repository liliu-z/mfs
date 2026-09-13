from __future__ import annotations

import threading
from collections.abc import Callable

from .types import UnderPath


class ScopeLease:
    """Temporarily retire work and source reads; release without changing user intent."""

    def __init__(self, scopes: tuple[UnderPath, ...], release: Callable[[], None]) -> None:
        self.scopes = scopes
        self._release: Callable[[], None] | None = release
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            if self._release is not None:
                release, self._release = self._release, None
                release()

    def __enter__(self) -> ScopeLease:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
