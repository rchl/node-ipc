"""
Python parent process that communicates with a Node.js child process
using Node's native IPC channel on Windows (named pipe + newline-delimited JSON).
"""

import json
import os
import subprocess
import sys
import threading
import uuid
from collections.abc import Callable
from typing import final

import pywintypes
import win32file
import win32pipe

PIPE_BUFFER = 65536


@final
class NodeIPCProcess:

    def __init__(self, script_path: str, args: list[str] | None = None):
        self.script_path = script_path
        self.args = args or []
        self._proc = None
        self._pipe_handle = None
        self._pipe_name = None
        self._message_handlers: list[Callable[[dict[str, object]], None]] = []
        self._reader_thread = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        # Unique named pipe — Node expects \\.\pipe\<name>
        self._pipe_name = f"\\\\.\\pipe\\node-ipc-{uuid.uuid4().hex}"

        # Create the named pipe (server side, Python is the server)
        self._pipe_handle = win32pipe.CreateNamedPipe(
            self._pipe_name,
            win32pipe.PIPE_ACCESS_DUPLEX,
            win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE | win32pipe.PIPE_WAIT,
            1,            # max instances
            PIPE_BUFFER,  # out buffer
            PIPE_BUFFER,  # in buffer
            0,            # default timeout
            None,         # default security
        )

        env = os.environ.copy()
        # On Windows, NODE_CHANNEL_FD holds the pipe *name*, not a numeric fd
        env["NODE_CHANNEL_FD"] = self._pipe_name
        env["NODE_CHANNEL_SERIALIZATION_MODE"] = "json"

        self._proc = subprocess.Popen(
            ["node", self.script_path] + self.args,
            stdin=subprocess.PIPE,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
        )

        # Block until Node connects to the pipe
        win32pipe.ConnectNamedPipe(self._pipe_handle, None)

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
                    # ERROR_BROKEN_PIPE (109) — Node disconnected cleanly
                    if e.winerror == 109:
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
    import time

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
