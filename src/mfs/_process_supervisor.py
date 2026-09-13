"""Standalone POSIX supervisor. Keep this module independent of MFS imports.

The command has its own process group. A pipe detects owner death; an inherited
flock descriptor keeps the instance unavailable until command retirement.
"""

from __future__ import annotations

import contextlib
import os
import select
import signal
import subprocess
import sys
import time
from collections.abc import Sequence


def _retire(process: subprocess.Popen[bytes]) -> int:
    # waitid(WNOWAIT) has not reaped our child: its PID cannot be reused here.
    while True:
        # macOS can return EPERM when only a zombie group leader remains.
        # Confirm retirement below; neither a missing nor an unsignalable group
        # alone proves its descendants are gone. Repeat for concurrent forks.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGKILL)
        try:
            listing = subprocess.check_output(
                ["/bin/ps", "-axo", "pgid=,stat="], text=True, encoding="ascii"
            )
            active = any(
                fields[0] == str(process.pid) and not fields[1].startswith("Z")
                for line in listing.splitlines()
                if len(fields := line.split()) >= 2
            )
            if not active:
                return process.wait()
        except (OSError, subprocess.SubprocessError):
            # Failure to confirm retirement must retain the instance lease.
            pass
        time.sleep(0.025)


def main(args: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if args is None else args)
    watch = int(args[0])
    process: subprocess.Popen[bytes] | None = None
    result = 1
    try:
        # Do not start work when the owner died before the supervisor booted.
        if select.select([watch], [], [], 0)[0] and not os.read(watch, 1):
            return 1
        process = subprocess.Popen(args[1:], stdin=subprocess.DEVNULL, start_new_session=True)
        while os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
            if select.select([watch], [], [], 0.025)[0] and not os.read(watch, 1):
                break
    finally:
        if process is not None:
            result = _retire(process)
        os.close(watch)
    return result if result >= 0 else 128 - result


if __name__ == "__main__":
    raise SystemExit(main())
