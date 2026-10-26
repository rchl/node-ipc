"""
Linux and macOS backend: Node's IPC channel is one end of a Unix socket
pair, whose fd number is passed in NODE_CHANNEL_FD. Bytes are sent as-is.
"""

import asyncio
import contextlib
import os
import socket
import sys

assert sys.platform != "win32"

READ_CHUNK_SIZE = 65536

Process = asyncio.subprocess.Process


async def spawn(script_path: str, args: list[str]) -> tuple[Process, asyncio.StreamReader, asyncio.StreamWriter]:
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
            proc = await asyncio.create_subprocess_exec(
                "node", script_path, *args,
                pass_fds=(child_fd,),
                stdin=asyncio.subprocess.DEVNULL,
                env=env,
            )
        except BaseException:
            parent_sock.close()
            raise

    try:
        reader, writer = await asyncio.open_unix_connection(sock=parent_sock)
    except BaseException:
        # Don't leave the child running if connecting fails or is cancelled;
        # the caller never gets the process, so nothing else can stop it.
        parent_sock.close()
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        await proc.wait()
        raise
    return proc, reader, writer


def encode(payload: bytes) -> bytes:
    """Bytes go over the socket unchanged."""
    return payload


async def read_chunk(reader: asyncio.StreamReader) -> bytes:
    """Return the next bytes from Node. Raises EOFError once the channel closes."""
    chunk = await reader.read(READ_CHUNK_SIZE)
    if not chunk:
        raise EOFError("IPC channel closed")
    return chunk
