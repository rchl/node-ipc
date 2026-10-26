"""
End-to-end test of the Python <-> Node IPC bridge.
"""

import asyncio
import os
import sys
import unittest
from typing import Any
from unittest import mock

from node_ipc import NodeIPCProcess

CHILD_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "child.js")
TIMEOUT = 15


class NodeIPCTest(unittest.IsolatedAsyncioTestCase):

    async def test_round_trip(self) -> None:
        # Timeout is outermost so it also covers the cleanup in __aexit__.
        async with asyncio.timeout(TIMEOUT), NodeIPCProcess(CHILD_SCRIPT) as proc:
            # Node -> Python: child sends a hello message after startup.
            self.assertEqual(await proc.receive(), {"type": "hello", "text": "message from child"})

            # Python -> Node: child exits only once it receives the exit message.
            for i in range(3):
                await proc.send({"type": "ping", "seq": i})
            await proc.send({"type": "exit"})

            self.assertEqual(await proc.wait(), 0)

            # Channel is closed once the child is gone.
            self.assertEqual([msg async for msg in proc], [])

    async def test_large_message(self) -> None:
        # Well above asyncio's 64 KiB default line limit and the pipe buffer size.
        data = "x" * (1024 * 1024)
        async with asyncio.timeout(TIMEOUT), NodeIPCProcess(CHILD_SCRIPT) as proc:
            await proc.send({"type": "echo", "data": data})
            # The child's hello may arrive before or after the echo.
            async for msg in proc:
                if msg["type"] == "echo":
                    break
            else:
                self.fail("no echo from child")
            self.assertEqual(msg["data"], data)

    async def test_arguments(self) -> None:
        # Each needs quoting or escaping on the Windows command line.
        args = ["with space", 'with "quotes"', "trailing backslash\\", "tab\there", ""]
        async with asyncio.timeout(TIMEOUT), NodeIPCProcess(CHILD_SCRIPT, args) as proc:
            await proc.send({"type": "argv"})
            async for msg in proc:
                if msg["type"] == "argv":
                    break
            else:
                self.fail("no argv from child")
            self.assertEqual(msg["argv"], args)

    async def test_start_twice(self) -> None:
        async with asyncio.timeout(TIMEOUT), NodeIPCProcess(CHILD_SCRIPT) as proc:
            with self.assertRaises(RuntimeError):
                await proc.start()
            # The first child is unaffected.
            await proc.send({"type": "exit"})
            self.assertEqual(await proc.wait(), 0)

    @unittest.skipIf(sys.platform == "win32", "the POSIX backend connects after spawning")
    async def test_failed_start_stops_child(self) -> None:
        spawned: list[asyncio.subprocess.Process] = []
        create_subprocess_exec = asyncio.create_subprocess_exec

        async def record_spawn(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
            proc = await create_subprocess_exec(*args, **kwargs)
            spawned.append(proc)
            return proc

        with (
            mock.patch("asyncio.create_subprocess_exec", record_spawn),
            mock.patch("asyncio.open_unix_connection", side_effect=OSError("connect failed")),
        ):
            async with asyncio.timeout(TIMEOUT):
                with self.assertRaises(OSError):
                    await NodeIPCProcess(CHILD_SCRIPT).start()

        self.assertEqual(len(spawned), 1)
        self.assertIsNotNone(spawned[0].returncode, "child is still running")


if __name__ == "__main__":
    unittest.main()
