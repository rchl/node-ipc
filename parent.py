"""
Python parent process that communicates with a Node.js child process
using Node's native IPC channel protocol (newline-delimited JSON over a
Unix socket whose fd number is passed in NODE_CHANNEL_FD).
"""

import asyncio
import contextlib
import json
import os
import socket
import sys
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Self, final


@final
class NodeIPCProcess:
    """
    Spawns a Node.js child process with an IPC channel,
    mimicking what Node's child_process.fork() sets up.

        async with NodeIPCProcess("child.js") as proc:
            await proc.send({"type": "ping"})
            async for message in proc:
                ...
    """

    _proc: asyncio.subprocess.Process | None = None
    _reader: asyncio.StreamReader
    _writer: asyncio.StreamWriter

    def __init__(self, script_path: str, args: list[str] | None = None):
        self.script_path = script_path
        self.args = args or []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> Self:
        """Spawn the Node child with one end of a socket pair as its IPC channel."""

        # Node needs a single fd that is both readable and writable,
        # so give it one end of a Unix socket pair and keep the other.
        parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)

        with child_sock:
            child_fd = child_sock.fileno()

            # NODE_CHANNEL_FD can be any fd number; pass_fds keeps the number
            # unchanged in the child, so no dup2 onto a fixed fd is needed.
            env = os.environ.copy()
            env["NODE_CHANNEL_FD"] = str(child_fd)
            env["NODE_CHANNEL_SERIALIZATION_MODE"] = "json"

            try:
                self._proc = await asyncio.create_subprocess_exec(
                    "node", self.script_path, *self.args,
                    pass_fds=(child_fd,),
                    stdin=asyncio.subprocess.DEVNULL,
                    env=env,
                )
            except BaseException:
                parent_sock.close()
                raise

        self._reader, self._writer = await asyncio.open_unix_connection(sock=parent_sock)
        return self

    async def close(self) -> None:
        """Terminate the child if it is still running and close the channel."""
        if not self._proc:
            return
        if self._proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                self._proc.terminate()
        await self._proc.wait()
        self._writer.close()
        with contextlib.suppress(ConnectionError):
            await self._writer.wait_closed()

    async def wait(self) -> int:
        """Wait for the child to exit and return its exit code."""
        assert self._proc, "process not started"
        return await self._proc.wait()

    async def __aenter__(self) -> Self:
        return await self.start()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    async def send(self, message: dict[str, object]) -> None:
        """Send a JSON message to the Node child (process.on('message', …))."""
        self._writer.write((json.dumps(message) + "\n").encode())
        await self._writer.drain()

    async def receive(self) -> dict[str, object]:
        """Wait for the next message from Node. Raises EOFError once the channel closes."""
        while True:
            line = await self._reader.readline()
            if not line:
                raise EOFError("IPC channel closed")
            line = line.strip()
            if not line:
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                print(f"[IPC] bad JSON from Node: {line!r}", file=sys.stderr)

    async def __aiter__(self) -> AsyncIterator[dict[str, object]]:
        """Iterate over messages from Node until the channel closes."""
        while True:
            try:
                yield await self.receive()
            except EOFError:
                return
