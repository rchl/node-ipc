"""
Python parent process that communicates with a Node.js child process
using Node's native IPC channel protocol (newline-delimited JSON on fd 3).
"""

import json
import os
import socket
import subprocess
import sys
import threading
from collections.abc import Callable
from typing import final


@final
class NodeIPCProcess:
    """
    Spawns a Node.js child process with an IPC channel on fd 3,
    mimicking what Node's child_process.fork() sets up.
    """

    def __init__(self, script_path: str, args: list[str] | None = None):
        self.script_path = script_path
        self.args = args or []
        self._proc = None
        self._ipc_read = None   # Python reads Node's writes  (pipe read end)
        self._ipc_write = None  # Python writes, Node reads   (pipe write end)
        self._message_handlers: list[Callable[[dict[str, object]], None]] = []
        self._reader_thread = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        """Spawn the Node child with fd 3 wired up as the IPC channel."""

        # Two pipes:
        #   ipc_in  — Python writes  → Node reads  (NODE_CHANNEL_FD read end = r_in)
        #   ipc_out — Node writes    → Python reads (NODE_CHANNEL_FD write end = w_out)
        r_in,  w_in  = os.pipe()   # Python →  Node
        r_out, w_out = os.pipe()   # Node   →  Python

        # Node expects a *single* fd (3) that is both readable and writable.
        # We emulate that by giving Node a socket-pair instead of two half-pipes.
        # Simplest portable approach: use a Unix socket pair.
        parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)

        # Close the plain pipes we opened (not needed now)
        for fd in (r_in, w_in, r_out, w_out):
            os.close(fd)

        child_fd = child_sock.fileno()

        # Build the env Node needs to activate process.send() / process.on('message')
        env = os.environ.copy()
        env["NODE_CHANNEL_FD"] = str(3)          # fd number inside the child
        env["NODE_CHANNEL_SERIALIZATION_MODE"] = "json"

        # dup2 child_sock onto fd 3 *before* Popen so that fd 3 exists
        # in this process and is included via pass_fds. preexec_fn lambda
        # expressions silently discard return values, so we do the dup2
        # here in the parent instead — it's safe because we close fd 3
        # in the parent immediately after fork via the parent_sock reference.
        if child_fd != 3:
            os.dup2(child_fd, 3)
            os.set_inheritable(3, True)
            child_sock.close()   # original fd no longer needed
            child_fd = 3

        self._proc = subprocess.Popen(
            ["node", self.script_path] + self.args,
            pass_fds=(child_fd,),   # fd 3 is now explicitly passed through
            stdin=subprocess.PIPE,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
        )

        # Close fd 3 in the parent — parent keeps its half via parent_sock
        os.close(3)

        # Parent keeps its half; child half can be closed here
        child_sock.close()
        self._ipc_sock = parent_sock
        self._ipc_file = parent_sock.makefile("r")  # buffered reader for newlines

        # Start background thread to dispatch inbound messages
        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

        return self

    def stop(self):
        if self._proc:
            self._proc.terminate()
            self._proc.wait()

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    def send(self, message: dict[str, object]):
        """Send a JSON message to the Node child (process.on('message', …))."""
        payload = json.dumps(message) + "\n"
        with self._lock:
            self._ipc_sock.sendall(payload.encode())

    def on_message(self, handler: Callable[[dict[str, object]], None]):
        """Register a callback invoked for every message from Node."""
        self._message_handlers.append(handler)
        return self  # chainable

    def wait(self):
        if self._proc:
            self._proc.wait()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _read_loop(self):
        try:
            for line in self._ipc_file:
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
# Demo — run this file directly to test the bridge
# ======================================================================

if __name__ == "__main__":
    import time

    received: list[dict[str, object]] = []

    def on_msg(msg: dict[str, object]) -> None:
        print(f"[Python] ← Node: {msg}")
        received.append(msg)

    proc = NodeIPCProcess("child.js").on_message(on_msg).start()

    # Give Node a moment to initialise
    time.sleep(0.3)

    for i in range(3):
        payload: dict[str, object] = {"type": "ping", "seq": i, "text": f"Hello from Python #{i}"}
        print(f"[Python] → Node: {payload}")
        proc.send(payload)
        time.sleep(0.4)

    proc.send({"type": "exit"})
    proc.wait()
    print(f"\n[Python] done. Received {len(received)} messages from Node.")
