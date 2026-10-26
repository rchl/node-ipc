"""
End-to-end test of the Python <-> Node IPC bridge.

Uses parent_windows.py on Windows and parent.py elsewhere.
"""

import os
import sys
import threading
import unittest

if sys.platform == "win32":
    from parent_windows import NodeIPCProcess
else:
    from parent import NodeIPCProcess

CHILD_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "child.js")
TIMEOUT = 15


class NodeIPCTest(unittest.TestCase):

    def test_round_trip(self) -> None:
        received: list[dict[str, object]] = []
        got_hello = threading.Event()

        def on_msg(msg: dict[str, object]) -> None:
            received.append(msg)
            if msg.get("type") == "hello":
                got_hello.set()

        proc = NodeIPCProcess(CHILD_SCRIPT).on_message(on_msg).start()
        try:
            # Node -> Python: child sends a hello message after startup.
            self.assertTrue(got_hello.wait(TIMEOUT), f"no hello from child, received: {received}")
            self.assertEqual(received[0], {"type": "hello", "text": "message from child"})

            # Python -> Node: child exits only once it receives the exit message.
            for i in range(3):
                proc.send({"type": "ping", "seq": i})
            proc.send({"type": "exit"})

            waiter = threading.Thread(target=proc.wait, daemon=True)
            waiter.start()
            waiter.join(TIMEOUT)
            self.assertFalse(waiter.is_alive(), "child did not exit after exit message")
        finally:
            proc.stop()


if __name__ == "__main__":
    unittest.main()
