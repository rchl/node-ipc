"""
Spawn a Node.js script from Python and talk to it over Node's native IPC
channel, the one child_process.fork() sets up.

Messages are newline-delimited JSON. How the channel is created and how
bytes travel over it differs per platform, so that part lives in a
backend module: _posix on Linux and macOS, _windows on Windows. Each
backend provides spawn(), encode() and read_chunk().
"""

import asyncio
import contextlib
import json
import sys
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Self, final

if sys.platform == "win32":
    from . import _windows as _backend
else:
    from . import _posix as _backend

__all__ = ["NodeIPCProcess"]


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

    _proc: _backend.Process | None = None
    _reader: asyncio.StreamReader
    _writer: asyncio.StreamWriter

    def __init__(self, script_path: str, args: list[str] | None = None):
        self.script_path = script_path
        self.args = args or []
        self._buffer = bytearray()
        self._scanned = 0  # bytes of _buffer already searched for a newline

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> Self:
        """Spawn the Node child with its IPC channel connected to us.

        Each instance runs one child: starting it again raises RuntimeError,
        even after close(). A start that failed can be retried.
        """
        if self._proc:
            raise RuntimeError("NodeIPCProcess already started")
        self._proc, self._reader, self._writer = await _backend.spawn(self.script_path, self.args)
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
        self._writer.write(_backend.encode((json.dumps(message) + "\n").encode()))
        await self._writer.drain()

    async def receive(self) -> dict[str, object]:
        """Wait for the next message from Node. Raises EOFError once the channel closes."""
        while True:
            line = (await self._read_line()).strip()
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

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _read_line(self) -> bytes:
        """Return the next line from Node without its newline.

        A line may span several reads, so data is buffered until a newline
        arrives, with no length limit (Node puts none on message size).
        Only newly read bytes are searched each time.
        """
        while (end := self._buffer.find(b"\n", self._scanned)) < 0:
            self._scanned = len(self._buffer)
            self._buffer += await _backend.read_chunk(self._reader)
        line = bytes(self._buffer[:end])
        del self._buffer[: end + 1]
        self._scanned = 0
        return line
