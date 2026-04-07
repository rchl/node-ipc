"""
Python parent process that communicates with a Node.js child process
using Node's native IPC channel on Windows.

Named pipe flow:
  - Python creates a named pipe and opens both ends.
  - The client end is DuplicateHandle'd into an inheritable copy.
  - That copy is wrapped in a CRT fd whose number goes into NODE_CHANNEL_FD.
  - Python retains the server end for its own reads/writes.
  - After Popen, Python closes the inheritable duplicate (child has its own copy).
"""

import ctypes
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
import win32con
import win32file
import win32pipe

PIPE_BUFFER = 65536
INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value


@final
class NodeIPCProcess:

    def __init__(self, script_path: str, args: list[str] | None = None):
        self.script_path = script_path
        self.args = args or []
        self._proc = None
        self._server_handle = None   # parent's end of the pipe (non-inheritable)
        self._message_handlers: list[Callable[[dict[str, object]], None]] = []
        self._reader_thread = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        pipe_name = f"\\\\.\\pipe\\node-ipc-{uuid.uuid4().hex}"
        cur_proc = win32api.GetCurrentProcess()

        # --- server end (parent keeps this, non-inheritable) ---
        self._server_handle = win32pipe.CreateNamedPipe(
            pipe_name,
            win32pipe.PIPE_ACCESS_DUPLEX,
            win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE | win32pipe.PIPE_WAIT,
            1,
            PIPE_BUFFER,
            PIPE_BUFFER,
            0,
            None,   # default security — not inheritable
        )

        # --- client end (will be handed to Node) ---
        # Open without inheritance first, then duplicate into an inheritable copy.
        client_handle = win32file.CreateFile(
            pipe_name,
            win32file.GENERIC_READ | win32file.GENERIC_WRITE,
            0,
            None,               # not inheritable yet
            win32file.OPEN_EXISTING,
            win32file.FILE_FLAG_OVERLAPPED,
            None,
        )

        # DuplicateHandle → inheritable copy for the child
        inheritable_client = win32api.DuplicateHandle(
            cur_proc,           # source process
            client_handle,      # handle to duplicate
            cur_proc,           # target process (still us — Popen will inherit it)
            0,                  # desired access (ignored when DUPLICATE_SAME_ACCESS)
            True,               # bInheritHandle
            win32con.DUPLICATE_SAME_ACCESS,
        )
        win32file.CloseHandle(client_handle)  # original non-inheritable copy no longer needed

        # Wrap the inheritable Win32 handle in a CRT fd.
        # msvcrt.open_osfhandle takes ownership — do NOT close inheritable_client separately.
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
            close_fds=False,    # allow fd inheritance on Windows
        )

        # Now that the child has inherited the fd, close our copy.
        # os.close releases the CRT fd and its underlying Win32 handle.
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
        """Send a JSON message to the Node child (process.on('message', …))."""
        payload = (json.dumps(message) + "\n").encode()
        with self._lock:
            win32file.WriteFile(self._server_handle, payload)

    def on_message(self, handler: Callable[[dict], None]):
        self._message_handlers.append(handler)
        return self

    def wait(self):
        self._proc.wait()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _read_loop(self):
        buf = b""
        try:
            while True:
                try:
                    _, data = win32file.ReadFile(self._server_handle, PIPE_BUFFER)
                except pywintypes.error as e:
                    if e.winerror == 109:   # ERROR_BROKEN_PIPE — Node exited cleanly
                        break
                    raise

                buf += data
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
