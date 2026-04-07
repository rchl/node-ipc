"""
Python parent process that communicates with a Node.js child process
using Node's native IPC channel on Windows.

Node's IPC uses a named pipe whose child-side handle must be available
as CRT file-descriptor 3 inside the child process (NODE_CHANNEL_FD=3).
Python's subprocess.Popen never sets STARTUPINFO.lpReserved2, so the
MSVCRT in the child never maps handle→fd 3.  We call CreateProcess
directly via ctypes and build the lpReserved2 buffer ourselves so that
the child's CRT initialises fd 3 from our pipe handle.

lpReserved2 format (MSVC CRT _ioinit):
    DWORD  nCount          – number of entries
    BYTE   flags[nCount]   – FOPEN=0x01, FPIPE=0x08 per fd
    HANDLE handles[nCount] – one HANDLE per fd (8 bytes on 64-bit)
"""

import ctypes
import ctypes.wintypes as wintypes
import json
import os
import struct
import sys
import threading
import time
import uuid
from collections.abc import Callable
from typing import final

import pywintypes
import win32api
import win32event
import win32file
import win32pipe

# -----------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------
PIPE_BUFFER = 65536

FOPEN              = 0x01
FPIPE              = 0x08
STARTF_USESTDHANDLES        = 0x00000100
CREATE_UNICODE_ENVIRONMENT  = 0x00000400
HANDLE_FLAG_INHERIT         = 0x00000001
INFINITE                    = 0xFFFFFFFF
STD_INPUT_HANDLE            = wintypes.DWORD(-10)
STD_OUTPUT_HANDLE           = wintypes.DWORD(-11)
STD_ERROR_HANDLE            = wintypes.DWORD(-12)
INVALID_HANDLE_VALUE        = wintypes.HANDLE(-1).value

# -----------------------------------------------------------------------
# ctypes declarations
# -----------------------------------------------------------------------
kernel32 = ctypes.windll.kernel32
kernel32.GetStdHandle.restype       = wintypes.HANDLE
kernel32.GetStdHandle.argtypes      = [wintypes.DWORD]
kernel32.CreateProcessW.restype     = wintypes.BOOL
kernel32.TerminateProcess.restype   = wintypes.BOOL
kernel32.WaitForSingleObject.restype = wintypes.DWORD
kernel32.CloseHandle.restype        = wintypes.BOOL
kernel32.SetHandleInformation.restype = wintypes.BOOL


class STARTUPINFOW(ctypes.Structure):
    _fields_ = [
        ("cb",            wintypes.DWORD),
        ("lpReserved",    wintypes.LPWSTR),
        ("lpDesktop",     wintypes.LPWSTR),
        ("lpTitle",       wintypes.LPWSTR),
        ("dwX",           wintypes.DWORD),
        ("dwY",           wintypes.DWORD),
        ("dwXSize",       wintypes.DWORD),
        ("dwYSize",       wintypes.DWORD),
        ("dwXCountChars", wintypes.DWORD),
        ("dwYCountChars", wintypes.DWORD),
        ("dwFillAttribute", wintypes.DWORD),
        ("dwFlags",       wintypes.DWORD),
        ("wShowWindow",   wintypes.WORD),
        ("cbReserved2",   wintypes.WORD),
        ("lpReserved2",   ctypes.POINTER(ctypes.c_byte)),
        ("hStdInput",     wintypes.HANDLE),
        ("hStdOutput",    wintypes.HANDLE),
        ("hStdError",     wintypes.HANDLE),
    ]


class PROCESS_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("hProcess",  wintypes.HANDLE),
        ("hThread",   wintypes.HANDLE),
        ("dwProcessId", wintypes.DWORD),
        ("dwThreadId",  wintypes.DWORD),
    ]


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _build_lpReserved2(ipc_handle: int) -> bytes:
    """
    Build the lpReserved2 / cbReserved2 buffer.

    We put 4 entries (fds 0-3):
      - fds 0-2: flag=0 so the CRT skips them; they come from hStdInput/Output/Error
      - fd 3:    FOPEN|FPIPE with our named-pipe client handle
    """
    n          = 4
    handle_sz  = ctypes.sizeof(ctypes.c_void_p)   # 8 on 64-bit Windows
    h_fmt      = "Q" if handle_sz == 8 else "I"
    mask       = (1 << (handle_sz * 8)) - 1

    flags   = bytes([0, 0, 0, FOPEN | FPIPE])
    handles = [0, 0, 0, ipc_handle & mask]

    buf = struct.pack("<I", n)          # DWORD count
    buf += flags                        # n flag bytes (no padding)
    for h in handles:                   # n HANDLE values
        buf += struct.pack(f"<{h_fmt}", h)
    return buf


def _make_env_block(env: dict[str, str]) -> "ctypes.Array[ctypes.c_wchar]":
    """Build a Unicode, double-null-terminated environment block."""
    block = "".join(f"{k}={v}\0" for k, v in env.items()) + "\0"
    return ctypes.create_unicode_buffer(block)


def _spawn_node(cmd: str, env: dict[str, str], ipc_handle: int) -> tuple[int, int]:
    """
    Spawn node.exe with the given command/env, exposing ipc_handle as CRT fd 3
    via STARTUPINFOW.lpReserved2.

    Returns (hProcess, pid).
    """
    reserved2_bytes = _build_lpReserved2(ipc_handle)
    reserved2_buf   = ctypes.create_string_buffer(reserved2_bytes)

    stdin_h  = kernel32.GetStdHandle(STD_INPUT_HANDLE)
    stdout_h = kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
    stderr_h = kernel32.GetStdHandle(STD_ERROR_HANDLE)

    # Ensure standard handles are inheritable (may already be; ignore errors)
    for h in (stdin_h, stdout_h, stderr_h):
        if h and h != INVALID_HANDLE_VALUE:
            kernel32.SetHandleInformation(h, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT)

    si = STARTUPINFOW()
    si.cb         = ctypes.sizeof(STARTUPINFOW)
    si.dwFlags    = STARTF_USESTDHANDLES
    si.hStdInput  = stdin_h
    si.hStdOutput = stdout_h
    si.hStdError  = stderr_h
    si.cbReserved2 = len(reserved2_bytes)
    si.lpReserved2 = ctypes.cast(reserved2_buf, ctypes.POINTER(ctypes.c_byte))

    pi       = PROCESS_INFORMATION()
    env_buf  = _make_env_block(env)
    cmd_buf  = ctypes.create_unicode_buffer(cmd)

    ok = kernel32.CreateProcessW(
        None,                       # lpApplicationName
        cmd_buf,                    # lpCommandLine (must be mutable)
        None,                       # lpProcessAttributes
        None,                       # lpThreadAttributes
        True,                       # bInheritHandles
        CREATE_UNICODE_ENVIRONMENT, # dwCreationFlags
        env_buf,                    # lpEnvironment (Unicode)
        None,                       # lpCurrentDirectory
        ctypes.byref(si),
        ctypes.byref(pi),
    )
    if not ok:
        raise ctypes.WinError(ctypes.get_last_error())

    kernel32.CloseHandle(pi.hThread)
    return pi.hProcess, pi.dwProcessId


class _ProcHandle:
    """Minimal process wrapper backed by a Win32 HANDLE."""

    def __init__(self, hProcess: int, pid: int) -> None:
        self._handle = hProcess
        self.pid     = pid

    def wait(self) -> None:
        if self._handle:
            kernel32.WaitForSingleObject(self._handle, INFINITE)
            kernel32.CloseHandle(self._handle)
            self._handle = None

    def terminate(self) -> None:
        if self._handle:
            kernel32.TerminateProcess(self._handle, 1)


# -----------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------

@final
class NodeIPCProcess:

    def __init__(self, script_path: str, args: list[str] | None = None):
        self.script_path = script_path
        self.args        = args or []
        self._proc: _ProcHandle | None = None
        self._server_handle = None
        self._message_handlers: list[Callable[[dict[str, object]], None]] = []
        self._reader_thread: threading.Thread | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> "NodeIPCProcess":
        pipe_name = f"\\\\.\\pipe\\node-ipc-{uuid.uuid4().hex}"
        cur_proc  = win32api.GetCurrentProcess()

        # Server end — FILE_FLAG_OVERLAPPED required to match libuv's async I/O
        self._server_handle = win32pipe.CreateNamedPipe(
            pipe_name,
            win32pipe.PIPE_ACCESS_DUPLEX | win32file.FILE_FLAG_OVERLAPPED,
            win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE | win32pipe.PIPE_WAIT,
            1,          # nMaxInstances
            PIPE_BUFFER,
            PIPE_BUFFER,
            0,
            None,       # non-inheritable
        )

        # Put server into listening state (overlapped → doesn't block)
        connect_ov = pywintypes.OVERLAPPED()
        connect_ov.hEvent = win32event.CreateEvent(None, True, False, None)
        try:
            win32pipe.ConnectNamedPipe(self._server_handle, connect_ov)
        except pywintypes.error as e:
            if e.winerror != 997:   # ERROR_IO_PENDING is expected
                raise

        # Client end — non-inheritable first, then duplicate as inheritable
        client_handle = win32file.CreateFile(
            pipe_name,
            win32file.GENERIC_READ | win32file.GENERIC_WRITE,
            0,
            None,
            win32file.OPEN_EXISTING,
            win32file.FILE_FLAG_OVERLAPPED,
            None,
        )
        inheritable_client = win32api.DuplicateHandle(
            cur_proc, client_handle, cur_proc,
            0,
            True,   # bInheritHandle
            2,      # DUPLICATE_SAME_ACCESS
        )
        win32file.CloseHandle(client_handle)

        # Wait for ConnectNamedPipe to complete (client CreateFile already triggered it)
        win32event.WaitForSingleObject(connect_ov.hEvent, win32event.INFINITE)

        env = os.environ.copy()
        env["NODE_CHANNEL_FD"] = "3"        # CRT fd 3 — set via lpReserved2 below
        env["NODE_CHANNEL_SERIALIZATION_MODE"] = "json"

        # Build command line (quote tokens that contain spaces)
        parts = ["node", self.script_path] + self.args
        cmd   = " ".join(f'"{p}"' if (" " in p or not p) else p for p in parts)

        ipc_handle_int = int(inheritable_client.handle)
        hProcess, _pid = _spawn_node(cmd, env, ipc_handle_int)

        # Child has inherited its copy of the handle — close the parent's copy
        win32file.CloseHandle(inheritable_client)

        self._proc = _ProcHandle(hProcess, _pid)

        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

        return self

    def stop(self) -> None:
        if self._proc:
            self._proc.terminate()
            self._proc.wait()
        if self._server_handle:
            win32file.CloseHandle(self._server_handle)
            self._server_handle = None

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    def send(self, message: dict) -> None:
        """Send a newline-delimited JSON message to the Node child."""
        payload = (json.dumps(message) + "\n").encode()
        ov = pywintypes.OVERLAPPED()
        ov.hEvent = win32event.CreateEvent(None, True, False, None)
        with self._lock:
            try:
                rc, _ = win32file.WriteFile(self._server_handle, payload, ov)
                if rc != 0:
                    win32event.WaitForSingleObject(ov.hEvent, win32event.INFINITE)
            except pywintypes.error as e:
                if e.winerror == 997:   # ERROR_IO_PENDING
                    win32event.WaitForSingleObject(ov.hEvent, win32event.INFINITE)
                else:
                    raise

    def on_message(self, handler: Callable[[dict], None]) -> "NodeIPCProcess":
        self._message_handlers.append(handler)
        return self

    def wait(self) -> None:
        if self._proc:
            self._proc.wait()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _read_loop(self) -> None:
        buf = b""
        try:
            while True:
                read_buf = bytearray(PIPE_BUFFER)
                ov = pywintypes.OVERLAPPED()
                ov.hEvent = win32event.CreateEvent(None, True, False, None)
                n = None
                try:
                    rc, _ = win32file.ReadFile(self._server_handle, read_buf, ov)
                    if rc == 0:
                        n = win32file.GetOverlappedResult(self._server_handle, ov, False)
                except pywintypes.error as e:
                    if e.winerror == 997:   # ERROR_IO_PENDING
                        win32event.WaitForSingleObject(ov.hEvent, win32event.INFINITE)
                        try:
                            n = win32file.GetOverlappedResult(self._server_handle, ov, False)
                        except pywintypes.error as e2:
                            if e2.winerror in (109, 232):   # pipe broken / closing
                                break
                            raise
                    elif e.winerror in (109, 232):
                        break
                    else:
                        raise
                if n is None or n == 0:
                    continue
                buf += bytes(read_buf[:n])
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        print(f"[IPC] bad JSON from Node: {line!r}", file=sys.stderr)
                        continue
                    for handler in self._message_handlers:
                        handler(msg)
        except Exception as exc:
            print(f"[IPC] reader error: {exc}", file=sys.stderr)


# ======================================================================
# Demo
# ======================================================================

if __name__ == "__main__":
    received: list[dict[str, object]] = []

    def on_msg(msg: dict[str, object]) -> None:
        print(f"[Python] ← Node: {msg}")
        received.append(msg)

    proc = NodeIPCProcess("child.js").on_message(on_msg).start()

    time.sleep(0.3)

    for i in range(3):
        payload: dict[str, object] = {"type": "ping", "seq": i, "text": f"Hello from Python #{i}"}
        print(f"[Python] → Node: {payload}")
        proc.send(payload)
        time.sleep(0.4)

    proc.send({"type": "exit"})
    proc.wait()
    print(f"\n[Python] done. Received {len(received)} messages from Node.")
