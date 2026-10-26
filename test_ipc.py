"""
End-to-end test of the Python <-> Node IPC bridge.

Uses parent_windows.py on Windows and parent.py elsewhere.
"""

import asyncio
import os
import sys
import unittest

if sys.platform == "win32":
    from parent_windows import NodeIPCProcess
else:
    from parent import NodeIPCProcess

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


if __name__ == "__main__":
    unittest.main()
