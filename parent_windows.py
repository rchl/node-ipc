"""
Python parent process that communicates with a Node.js child process
using Node's native IPC channel on Windows.

libuv (Node's I/O layer) opens named pipes with FILE_FLAG_OVERLAPPED.
Windows requires BOTH ends of a pipe to agree on overlapped vs sync mode,
so the server end must also be created with FILE_FLAG_OVERLAPPED.
We use overlapped ReadFile/WriteFile with win32event objects on the Python side.
"""

import json
import msvcrt
import os
import subprocess
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

PIPE_BUFFER = 65536


@final
class NodeIPCProcess:

    def __init__(self, script_path: str, args: list[str] | None = None):
        self.script_path = script_path
        self.args = args or []
        self._proc = None
        self._server_handle = None
        self._message_handlers: list[Callable[[dict[str, object]], None]] = []
        self._reader_thread = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        pipe_name = f"\\\\.\\pipe\\node-ipc-{uuid.uuid4().hex}"
        cur_proc = win32api.GetCurrentProcess()

        # Server end — FILE_FLAG_OVERLAPPED is required to match libuv's client end
        self._server_handle = win32pipe.CreateNamedPipe(
            pipe_name,
            win32pipe.PIPE_ACCESS_DUPLEX | win32file.FILE_FLAG_OVERLAPPED,
            win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE | win32pipe.PIPE_WAIT,
            1,
            PIPE_BUFFER,
            PIPE_BUFFER,
            0,
            None,   # non-inheritable
        )

        # Client end — also overlapped (libuv requirement), non-inheritable first
        client_handle = win32file.CreateFile(
            pipe_name,
            win32file.GENERIC_READ | win32file.GENERIC_WRITE,
            0,
            None,
            win32file.OPEN_EXISTING,
            win32file.FILE_FLAG_OVERLAPPED,
            None,
        )

        # Duplicate into an inheritable handle for the child process
        inheritable_client = win32api.DuplicateHandle(
            cur_proc,
            client_handle,
            cur_proc,
            0,
            True,   # bInheritHandle
            2,      # DUPLICATE_SAME_ACCESS
        )
        win32file.CloseHandle(client_handle)

        # Wrap the inheritable handle as a CRT fd — Node reads NODE_CHANNEL_FD as int
        child_fd = msvcrt.open_osfhandle(
            inheritable_client.handle,
            os.O_RDWR | os.O_BINARY,
        )

        env = os.environ.copy()
        env["NODE_CHANNEL_FD"] = str(child_fd)
        env["NODE_CHANNEL_SERIALIZATION_MODE"] = "json"

        self._proc = subprocess.Popen(
            ["node", self.script_path] + self.args,
            stdin=subprocess.PIPE,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
            close_fds=False,
        )

        # Child has inherited its copy — close ours
        os.close(child_fd)

        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

        return self

    def stop(self):
        if self._proc:
            self._proc.terminate()
            self._proc.wait()
        if self._server_handle:
            win32file.CloseHandle(self._server_handle)
            self._server_handle = None

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    def send(self, message: dict):
        """Send a length-prefixed JSON frame to the Node child."""
        body = json.dumps(message).encode()
        # 4-byte little-endian length header + JSON body (no newline)
        payload = len(body).to_bytes(4, "little") + body
        overlapped = pywintypes.OVERLAPPED()
        overlapped.hEvent = win32event.CreateEvent(None, True, False, None)
        with self._lock:
            try:
                win32file.WriteFile(self._server_handle, payload, overlapped)
            except pywintypes.error as e:
                if e.winerror != 997:   # ERROR_IO_PENDING is expected for overlapped I/O
                    raise
            win32event.WaitForSingleObject(overlapped.hEvent, win32event.INFINITE)

    def on_message(self, handler: Callable[[dict], None]):
        self._message_handlers.append(handler)
        return self

    def wait(self):
        self._proc.wait()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _overlapped_read(self, nbytes: int) -> bytes | None:
        """Blocking overlapped read of exactly nbytes. Returns None on pipe close."""
        buf = b""
        while len(buf) < nbytes:
            overlapped = pywintypes.OVERLAPPED()
            overlapped.hEvent = win32event.CreateEvent(None, True, False, None)
            try:
                _, chunk = win32file.ReadFile(
                    self._server_handle, nbytes - len(buf), overlapped
                )
            except pywintypes.error as e:
                if e.winerror != 997:
                    if e.winerror in (109, 232):
                        return None
                    raise
                chunk = b""
            if not chunk:
                win32event.WaitForSingleObject(overlapped.hEvent, win32event.INFINITE)
                try:
                    n = win32file.GetOverlappedResult(self._server_handle, overlapped, False)
                    _, chunk = win32file.ReadFile(self._server_handle, n)
                except pywintypes.error as e:
                    if e.winerror in (109, 232):
                        return None
                    raise
            buf += chunk
        return buf

    def _read_loop(self):
        try:
            while True:
                # Read 4-byte little-endian length header
                header = self._overlapped_read(4)
                if header is None:
                    break
                msg_len = int.from_bytes(header, "little")

                # Read exactly msg_len bytes of JSON body
                body = self._overlapped_read(msg_len)
                if body is None:
                    break

                try:
                    msg = json.loads(body.decode())
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    print(f"[IPC] bad frame from Node: {e}", file=sys.stderr)
                    continue
                for handler in self._message_handlers:
                    handler(msg)
        except Exception as e:
            print(f"[IPC] reader error: {e}", file=sys.stderr)


# ======================================================================
# Demo
# ======================================================================

if __name__ == "__main__":
    received: list[dict[str, object]] = []

    def on_msg(msg: dict[str, object]):
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
