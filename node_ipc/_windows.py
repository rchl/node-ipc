"""
Windows backend for Node's native IPC channel.

Node's IPC uses a named pipe whose child-side handle must be available
as CRT file-descriptor 3 inside the child process (NODE_CHANNEL_FD=3).
Python's subprocess.Popen never sets STARTUPINFO.lpReserved2, so the
MSVCRT in the child never maps handle→fd 3.  We call CreateProcess
directly via ctypes and build the lpReserved2 buffer ourselves so that
the child's CRT initialises fd 3 from our pipe handle.

lpReserved2 format (MSVC CRT _ioinit):
    DWORD  nCount          - number of entries
    BYTE   flags[nCount]   - FOPEN=0x01, FPIPE=0x08 per fd
    HANDLE handles[nCount] - one HANDLE per fd (8 bytes on 64-bit)

The parent end of the pipe is driven by asyncio: the child inherits the
server end, and the parent connects as the client through the
ProactorEventLoop (the default event loop on Windows). libuv wraps every
write in a frame header, so bytes are framed on the way out and unframed
on the way in.
"""

import asyncio
import ctypes
import ctypes.wintypes as wintypes
import os
import struct
import subprocess
import sys
import uuid

import pywintypes
import win32file
import win32pipe

assert sys.platform == "win32"

# -----------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------
PIPE_BUFFER = 65536

FOPEN              = 0x01
FPIPE              = 0x08
STARTF_USESTDHANDLES        = 0x00000100
CREATE_UNICODE_ENVIRONMENT  = 0x00000400
HANDLE_FLAG_INHERIT         = 0x00000001
FILE_FLAG_FIRST_PIPE_INSTANCE = 0x00080000
INFINITE                    = 0xFFFFFFFF
STD_INPUT_HANDLE            = wintypes.DWORD(-10)
STD_OUTPUT_HANDLE           = wintypes.DWORD(-11)
STD_ERROR_HANDLE            = wintypes.DWORD(-12)
INVALID_HANDLE_VALUE        = wintypes.HANDLE(-1).value

# libuv IPC frame layout (uv__ipc_frame_header_t + optional socket xfer + data):
#     [16-byte header: flags u32, reserved u32, data_length u32, reserved u32]
#     [632-byte socket-xfer block]  ← only if flags & 0x02
#     [data_length bytes payload]   ← only if flags & 0x01
IPC_HEADER_SIZE      = 16
IPC_SOCKET_XFER_SIZE = 632
IPC_HAS_DATA         = 0x01
IPC_HAS_SOCKET_XFER  = 0x02

# -----------------------------------------------------------------------
# ctypes declarations
# -----------------------------------------------------------------------
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
kernel32.GetStdHandle.restype       = wintypes.HANDLE
kernel32.GetStdHandle.argtypes      = [wintypes.DWORD]
kernel32.CreateProcessW.restype     = wintypes.BOOL
kernel32.TerminateProcess.restype   = wintypes.BOOL
kernel32.TerminateProcess.argtypes  = [wintypes.HANDLE, wintypes.UINT]
kernel32.WaitForSingleObject.restype = wintypes.DWORD
kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
kernel32.GetExitCodeProcess.restype = wintypes.BOOL
kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
kernel32.CloseHandle.restype        = wintypes.BOOL
kernel32.CloseHandle.argtypes       = [wintypes.HANDLE]
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
      - fd 3:    FOPEN|FPIPE with our named-pipe handle
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


def _make_env_block(env: dict[str, str]) -> ctypes.Array[ctypes.c_wchar]:
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


class Process:
    """Minimal asyncio-friendly process wrapper backed by a Win32 HANDLE."""

    def __init__(self, hProcess: int, pid: int) -> None:
        self._handle = hProcess
        self.pid     = pid
        self.returncode: int | None = None
        self._waiter: asyncio.Task[int] | None = None

    async def wait(self) -> int:
        if self._waiter is None:
            self._waiter = asyncio.create_task(self._wait())
        return await asyncio.shield(self._waiter)

    async def _wait(self) -> int:
        await asyncio.to_thread(kernel32.WaitForSingleObject, self._handle, INFINITE)
        code = wintypes.DWORD()
        kernel32.GetExitCodeProcess(self._handle, ctypes.byref(code))
        kernel32.CloseHandle(self._handle)
        self.returncode = code.value
        return code.value

    def terminate(self) -> None:
        if self.returncode is None:
            kernel32.TerminateProcess(self._handle, 1)


# -----------------------------------------------------------------------
# Backend API
# -----------------------------------------------------------------------

async def spawn(script_path: str, args: list[str]) -> tuple[Process, asyncio.StreamReader, asyncio.StreamWriter]:
    """Spawn the Node child with the server end of a named pipe as CRT fd 3."""
    loop = asyncio.get_running_loop()
    if not isinstance(loop, asyncio.ProactorEventLoop):
        raise RuntimeError("NodeIPCProcess requires the ProactorEventLoop")

    pipe_name = f"\\\\.\\pipe\\node-ipc-{uuid.uuid4().hex}"

    # Server end goes to the child, so create it inheritable.
    # FILE_FLAG_OVERLAPPED is required to match libuv's async I/O.
    sa = pywintypes.SECURITY_ATTRIBUTES()
    sa.bInheritHandle = True
    server_handle = win32pipe.CreateNamedPipe(
        pipe_name,
        win32pipe.PIPE_ACCESS_DUPLEX | win32file.FILE_FLAG_OVERLAPPED | FILE_FLAG_FIRST_PIPE_INSTANCE,
        win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE | win32pipe.PIPE_WAIT,
        1,          # nMaxInstances
        PIPE_BUFFER,
        PIPE_BUFFER,
        0,
        sa,
    )
    try:
        # Connect our end as the client through the proactor.
        reader = asyncio.StreamReader()
        transport, protocol = await loop.create_pipe_connection(
            lambda: asyncio.StreamReaderProtocol(reader), pipe_name,
        )
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)

        env = os.environ.copy()
        env["NODE_CHANNEL_FD"] = "3"        # CRT fd 3 — set via lpReserved2
        env["NODE_CHANNEL_SERIALIZATION_MODE"] = "json"

        # Quote and escape per the MSVC C runtime rules that Node parses argv with
        cmd = subprocess.list2cmdline(["node", script_path, *args])

        try:
            hProcess, pid = _spawn_node(cmd, env, int(server_handle))
        except BaseException:
            writer.close()
            raise
    finally:
        # The child has inherited its own copy of the server handle
        win32file.CloseHandle(server_handle)

    return Process(hProcess, pid), reader, writer


def encode(payload: bytes) -> bytes:
    """Wrap a payload in a libuv IPC frame.

    libuv IPC frame layout (uv__ipc_frame_header_t, 16 bytes):
        uint32_t flags        - UV__IPC_FRAME_HAS_DATA = 0x01
        uint32_t reserved1    - 0
        uint32_t data_length  - byte length of the payload
        uint32_t reserved2    - 0
    Immediately followed by data_length bytes of payload.
    """
    return struct.pack("<IIII", IPC_HAS_DATA, 0, len(payload), 0) + payload


async def read_chunk(reader: asyncio.StreamReader) -> bytes:
    """Read one libuv IPC frame and return its payload. Raises EOFError once the channel closes."""
    try:
        header = await reader.readexactly(IPC_HEADER_SIZE)
        flags, _r1, data_length, _r2 = struct.unpack("<IIII", header)
        if flags & IPC_HAS_SOCKET_XFER:
            await reader.readexactly(IPC_SOCKET_XFER_SIZE)
        if flags & IPC_HAS_DATA:
            return await reader.readexactly(data_length)
        return b""
    except asyncio.IncompleteReadError as e:
        raise EOFError("IPC channel closed") from e
