# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import time
from functools import cache
from pathlib import Path
from typing import Any, cast


def fsync_directory(path: Path) -> None:
    if os.name == "nt":
        # Windows does not expose fsync on directory descriptors. Files themselves
        # are flushed before replace; SQLite uses its native Windows durable VFS.
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class ProcessTree:
    def __init__(self, process: subprocess.Popen[bytes]) -> None:
        self.process = process
        self._job: Any = None
        self._kernel: Any = None
        self._accounting_type: Any = None
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes

            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.CreateJobObjectW.restype = wintypes.HANDLE
            kernel.SetInformationJobObject.argtypes = [
                wintypes.HANDLE,
                ctypes.c_int,
                ctypes.c_void_p,
                wintypes.DWORD,
            ]
            kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
            kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]

            class Basic(ctypes.Structure):
                _fields_ = [
                    ("per_process", ctypes.c_int64),
                    ("per_job", ctypes.c_int64),
                    ("flags", wintypes.DWORD),
                    ("min_ws", ctypes.c_size_t),
                    ("max_ws", ctypes.c_size_t),
                    ("active", wintypes.DWORD),
                    ("affinity", ctypes.c_size_t),
                    ("priority", wintypes.DWORD),
                    ("scheduling", wintypes.DWORD),
                ]

            class Extended(ctypes.Structure):
                _fields_ = [
                    ("basic", Basic),
                    ("io", ctypes.c_uint64 * 6),
                    ("process_memory", ctypes.c_size_t),
                    ("job_memory", ctypes.c_size_t),
                    ("peak_process", ctypes.c_size_t),
                    ("peak_job", ctypes.c_size_t),
                ]

            class Accounting(ctypes.Structure):
                _fields_ = [
                    ("times", ctypes.c_int64 * 4),
                    ("faults", wintypes.DWORD),
                    ("total", wintypes.DWORD),
                    ("active", wintypes.DWORD),
                    ("terminated", wintypes.DWORD),
                ]

            kernel.QueryInformationJobObject.argtypes = [
                wintypes.HANDLE,
                ctypes.c_int,
                ctypes.c_void_p,
                wintypes.DWORD,
                ctypes.c_void_p,
            ]
            self._accounting_type = Accounting
            job = kernel.CreateJobObjectW(None, None)
            if not job:
                raise ctypes.WinError(ctypes.get_last_error())
            limits = Extended()
            limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            try:
                if not kernel.SetInformationJobObject(
                    job, 9, ctypes.byref(limits), ctypes.sizeof(limits)
                ) or not kernel.AssignProcessToJobObject(job, int(cast(Any, process)._handle)):
                    raise ctypes.WinError(ctypes.get_last_error())
                # The caller creates it suspended so no child can escape assignment.
                ntdll = ctypes.WinDLL("ntdll")
                ntdll.NtResumeProcess.argtypes = [wintypes.HANDLE]
                if ntdll.NtResumeProcess(int(cast(Any, process)._handle)) != 0:
                    raise OSError("could not resume managed process")
            except BaseException:
                kernel.CloseHandle(job)
                raise
            self._job = job
            self._kernel = kernel

    def close(self) -> None:
        if self._job is not None:
            import ctypes

            if not self._kernel.TerminateJobObject(self._job, 1):
                raise OSError("could not terminate managed process job")
            self.process.wait()
            # ActiveProcesses can retain a terminated process until open process
            # references are released. Popen retains its Windows handle by default.
            cast(Any, self.process)._handle.Close()
            while True:
                accounting = self._accounting_type()
                if not self._kernel.QueryInformationJobObject(
                    self._job, 1, ctypes.byref(accounting), ctypes.sizeof(accounting), None
                ):
                    raise OSError("could not observe managed process retirement")
                if accounting.active == 0:
                    break
                time.sleep(0.005)
            self._kernel.CloseHandle(self._job)
            self._job = None
        elif os.name == "posix":
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGKILL)
        elif self.process.poll() is None:
            self.process.kill()


class WindowsFiles:
    """Handle-pinned Windows equivalent of the descriptor operations used by sync.

    Every opened entry forbids deletion/rename until its descriptor closes.
    Reparse points are rejected before their contents can be read. Canonical
    paths are taken from the opened handle, never reconstructed from user input.
    """

    def __init__(self) -> None:
        import ctypes
        from ctypes import wintypes

        self.ctypes: Any = ctypes
        self.kernel = self.ctypes.WinDLL("kernel32", use_last_error=True)
        self.kernel.CreateFileW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            ctypes.c_void_p,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        self.kernel.CreateFileW.restype = wintypes.HANDLE
        self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        self.kernel.GetFinalPathNameByHandleW.argtypes = [
            wintypes.HANDLE,
            wintypes.LPWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
        ]
        self.kernel.GetFileInformationByHandleEx.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        ]

    def path(self, descriptor: int) -> Path:
        import msvcrt

        buffer = self.ctypes.create_unicode_buffer(32768)
        length = self.kernel.GetFinalPathNameByHandleW(
            msvcrt.get_osfhandle(  # pyright: ignore[reportAttributeAccessIssue]
                descriptor
            ),
            buffer,
            len(buffer),
            0,
        )
        if not length or length >= len(buffer):
            raise self.ctypes.WinError(self.ctypes.get_last_error())
        value = buffer.value
        if value.startswith("\\\\?\\UNC\\"):
            value = "\\\\" + value[8:]
        elif value.startswith("\\\\?\\"):
            value = value[4:]
        return Path(value)

    def open(self, path: str | Path, flags: int, *, dir_fd: int | None = None) -> int:
        import msvcrt

        del flags
        candidate = self.path(dir_fd) / path if dir_fd is not None else Path(path)
        handle = self.kernel.CreateFileW(str(candidate), 0x80000000, 3, None, 3, 0x02200000, None)
        if handle == self.ctypes.c_void_p(-1).value:
            raise self.ctypes.WinError(self.ctypes.get_last_error())
        try:
            tag = (self.ctypes.c_uint32 * 2)()
            if not self.kernel.GetFileInformationByHandleEx(
                handle, 9, self.ctypes.byref(tag), self.ctypes.sizeof(tag)
            ):
                raise self.ctypes.WinError(self.ctypes.get_last_error())
            if tag[0] & 0x400:
                raise OSError("reparse point traversal is not allowed")
            return msvcrt.open_osfhandle(  # pyright: ignore[reportAttributeAccessIssue]
                handle, os.O_RDONLY | getattr(os, "O_BINARY", 0)
            )
        except BaseException:
            self.kernel.CloseHandle(handle)
            raise

    def change_time(self, descriptor: int) -> int:
        import ctypes
        import msvcrt

        class BasicInfo(ctypes.Structure):
            _fields_ = [("times", ctypes.c_int64 * 4), ("attributes", ctypes.c_uint32)]

        information = BasicInfo()
        handle = msvcrt.get_osfhandle(descriptor)  # pyright: ignore[reportAttributeAccessIssue]
        if not self.kernel.GetFileInformationByHandleEx(
            handle, 0, ctypes.byref(information), ctypes.sizeof(information)
        ):
            raise self.ctypes.WinError(self.ctypes.get_last_error())
        return int(information.times[3])

    def scandir(self, descriptor: int) -> Any:
        return os.scandir(self.path(descriptor))

    def stat(self, path: str, *, dir_fd: int, follow_symlinks: bool = False) -> os.stat_result:
        return os.stat(self.path(dir_fd) / path, follow_symlinks=follow_symlinks)


def validate_windows_relative(path: str) -> None:
    from .errors import InvalidPath

    if path == ".":
        return
    reserved = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }
    for part in path.split("/"):
        if ":" in part or part.endswith((".", " ")) or part.split(".")[0].upper() in reserved:
            raise InvalidPath(
                "Windows device names, alternate streams and trailing dots/spaces "
                "are not document paths"
            )


@cache
def windows_files() -> WindowsFiles:
    return WindowsFiles()


def descriptor_change_time(descriptor: int) -> int:
    # Windows stat.ctime is creation time. FILE_BASIC_INFO.ChangeTime detects
    # metadata updates, including restoring LastWriteTime during a source read.
    # https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_basic_info
    return (
        windows_files().change_time(descriptor)
        if os.name == "nt"
        else os.fstat(descriptor).st_ctime_ns
    )
