from __future__ import annotations

import math
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Protocol, cast

from .errors import InvalidConfiguration


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    """Bound stage concurrency and each of the ranked-search and grep pools.

    Resource names may be shared with a host; adapter limits span both query pools.
    """

    workers: int = 4
    queries: int = 4
    resources: Mapping[str, int] = field(default_factory=lambda: {"heavy": 1, "light": 2})
    stage_timeout: float = 300.0

    def __post_init__(self) -> None:
        if (
            isinstance(self.stage_timeout, bool)
            or not isinstance(cast(object, self.stage_timeout), (int, float))
            or not math.isfinite(self.stage_timeout)
            or self.stage_timeout <= 0
        ):
            raise InvalidConfiguration("stage_timeout must be finite and positive")
        for value in (self.workers, self.queries, *self.resources.values()):
            if isinstance(value, bool) or not isinstance(cast(object, value), int) or value < 1:
                raise InvalidConfiguration("execution capacities must be positive integers")
        if any(not isinstance(cast(object, k), str) or not k for k in self.resources):
            raise InvalidConfiguration("resource names must be non-empty strings")


class ResourceLease(Protocol):
    def release(self) -> None: ...


class Admission(Protocol):
    """Nonblocking, all-or-nothing resource admission.

    A host adapter must retain grants until the actual invocation retires, including
    after caller timeout/disconnection. try_acquire must never perform blocking RPC.
    """

    def try_acquire(self, resources: Mapping[str, int]) -> ResourceLease | None: ...


class ResourceGrant:
    def __init__(self, release: Callable[[], None]) -> None:
        self._release = release
        self._lock = threading.Lock()
        self._released = False

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        self._release()


class LocalAdmission:
    """Share one instance between MFS and other in-process resource consumers."""

    def __init__(self, capacities: Mapping[str, int]) -> None:
        ExecutionPolicy(resources=capacities)
        self._capacities = dict(capacities)
        self._used: dict[str, int] = {}
        self._lock = threading.Lock()

    def try_acquire(self, resources: Mapping[str, int]) -> ResourceLease | None:
        requested = dict(resources)
        with self._lock:
            for name, count in requested.items():
                capacity = self._capacities.get(name)
                if (
                    capacity is None
                    or isinstance(count, bool)
                    or not isinstance(cast(object, count), int)
                    or not 0 < count <= capacity
                ):
                    raise InvalidConfiguration(f"invalid or unconfigured resource: {name}")
                if self._used.get(name, 0) + count > capacity:
                    return None
            for name, count in requested.items():
                self._used[name] = self._used.get(name, 0) + count

        def release() -> None:
            with self._lock:
                for name, count in requested.items():
                    self._used[name] -= count

        return ResourceGrant(release)
