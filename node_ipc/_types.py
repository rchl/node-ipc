"""
The contract every platform backend module (_posix, _windows) fulfils.

A module can satisfy a Protocol, so __init__ assigns the selected backend
to a variable typed as Backend and the type checker verifies it matches.
"""

import asyncio
from typing import Protocol


class Process(Protocol):
    """The running Node child, as returned by Backend.spawn()."""

    @property
    def returncode(self) -> int | None: ...

    async def wait(self) -> int: ...

    def terminate(self) -> None: ...


class Backend(Protocol):
    async def spawn(
        self, script_path: str, args: list[str]
    ) -> tuple[Process, asyncio.StreamReader, asyncio.StreamWriter]:
        """Start `node script_path *args` with its IPC channel connected to the returned streams."""
        ...

    def encode(self, payload: bytes) -> bytes:
        """Prepare outgoing bytes for the channel."""
        ...

    async def read_chunk(self, reader: asyncio.StreamReader) -> bytes:
        """Return the next incoming bytes. Raises EOFError once the channel closes."""
        ...
