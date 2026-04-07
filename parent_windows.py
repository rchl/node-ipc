"""
Python parent process that communicates with a Node.js child process
using Node's native IPC channel on Windows.

Node on Windows still expects NODE_CHANNEL_FD to be a numeric CRT fd,
not a pipe name. We create a named pipe, obtain a CRT fd from its Win32
handle via msvcrt.open_osfhandle, and pass that fd number to the child.
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
import win32file
import win32pipe
import win32security

PIPE_BUFFER = 65536


@final
class NodeIPCProcess:

    def __init__(self, script_path: str, args: list[str] | None = None):
        self.script_path = script_path
        self.args = args or []
        self._proc = None
        self._pipe_handle = None       # parent's Win32 handle (read/write)
        self._child_handle = None      # child's inheritable Win32 handle
        self._child_fd = None          # CRT fd number passed to Node
        self._message_handlers: list[Callable[[dict[str, object]], None]] = []
        self._reader_thread = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        pipe_name = f"\\\\.\\pipe\\node-ipc-{uuid.uuid4().hex}"

        # Security attributes — mark the child-side handle as inheritable
        sa = win32security.SECURITY_ATTRIBUTES()
        sa.bInheritHandle = True

        # Create the named pipe server (parent side, non-inheritable)
        self._pipe_handle = win32pipe.CreateNamedPipe(
            pipe_name,
            win32pipe.PIPE_ACCESS_DUPLEX,
            win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE | win32pipe.PIPE_WAIT,
            1,            # max instances
            PIPE_BUFFER,
            PIPE_BUFFER,
            0,
            None,         # non-inheritable (parent keeps this end)
        )

        # Open the client end of the pipe with an inheritable handle for the child
        self._child_handle = win32file.CreateFile(
            pipe_name,
            win32file.GENERIC_READ | win32file.GENERIC_WRITE,
            0,
            sa,           # inheritable
            win32file.OPEN_EXISTING,
            0,
            None,
        )

        # Convert the inheritable Win32 handle to a CRT fd number.
        # os.O_RDWR | os.O_BINARY matches what Node expects on the fd.
        self._child_fd = msvcrt.open_osfhandle(
            self._child_handle.handle,
            os.O_RDWR | os.O_BINARY,
        )

        env = os.environ.copy()
        env["NODE_CHANNEL_FD"] = str(self._child_fd)
        env["NODE_CHANNEL_SERIALIZATION_MODE"] = "json"

        self._proc = subprocess.Popen(
            ["node", self.script_path] + self.args,
            stdin=subprocess.PIPE,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
            close_fds=False,   # allow fd inheritance on Windows
        )

        # Parent no longer needs the child-side fd — close it here so that
        # Node's disconnect detection (EOF/broken-pipe) works correctly.
        os.close(self._child_fd)
        self._child_fd = None
        self._child_handle = None  # handle is now owned by the CRT fd (closed above)

        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

        return self

    def stop(self):
        if self._proc:
            self._proc.terminate()
            self._proc.wait()
        if self._pipe_handle:
            win32file.CloseHandle(self._pipe_handle)
            self._pipe_handle = None

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    def send(self, message: dict[str, object]):
        """Send a JSON message to the Node child (process.on('message', …))."""
        payload = (json.dumps(message) + "\n").encode()
        with self._lock:
            win32file.WriteFile(self._pipe_handle, payload)

    def on_message(self, handler: Callable[[dict[str, object]], None]):
        self._message_handlers.append(handler)
        return self

    def wait(self):
        if self._proc:
            self._proc.wait()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _read_loop(self):
        buf = b""
        try:
            while True:
                try:
                    _, data = win32file.ReadFile(self._pipe_handle, PIPE_BUFFER)
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
