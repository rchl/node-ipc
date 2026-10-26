# node-ipc

Spawn a Node.js script from Python and talk to it over Node's built-in IPC channel, the same one `child_process.fork()` sets up.

The child is a plain Node script: it uses `process.send()` and `process.on('message')` as if a Node parent had forked it. No extra npm packages and no stdin/stdout protocol are needed.

```python
import asyncio
from parent import NodeIPCProcess  # parent_windows on Windows

async def main():
    async with NodeIPCProcess("child.js") as proc:
        print(await proc.receive())    # {'type': 'hello', 'text': 'message from child'}
        await proc.send({"type": "exit"})
        print(await proc.wait())       # 0

asyncio.run(main())
```

## Requirements

- Python 3.14 or newer (see `.python-version`)
- Node.js on `PATH` as `node`. CI tests Node 20, 22 and 24.
- Windows only: [pywin32](https://pypi.org/project/pywin32/), installed automatically by `uv sync`

```sh
uv sync
```

## API

`parent.py` (Linux and macOS) and `parent_windows.py` (Windows) each export a `NodeIPCProcess` class with the same API. Pick one based on the platform:

```python
import sys

if sys.platform == "win32":
    from parent_windows import NodeIPCProcess
else:
    from parent import NodeIPCProcess
```

| Member | Description |
|---|---|
| `NodeIPCProcess(script_path, args=None)` | Set up a child that runs `node script_path *args`. Nothing is spawned until it is started. |
| `async with NodeIPCProcess(...) as proc` | Start the child on entry. On exit, terminate it if it is still running and close the channel. |
| `await proc.start()` / `await proc.close()` | The same steps as `async with`, without the context manager. |
| `await proc.send(message)` | Send a JSON-serializable dict. The child receives it in `process.on('message')`. |
| `await proc.receive()` | Wait for the next message the child sent with `process.send()`. Raises `EOFError` once the channel is closed. |
| `async for message in proc` | Iterate over incoming messages until the channel closes. |
| `await proc.wait()` | Wait for the child to exit and return its exit code. |

The child's stdout and stderr are inherited from the Python process.

## How it works

Node activates the IPC channel when it starts with `NODE_CHANNEL_FD` set to a file descriptor. Messages then go over that descriptor. Both parents also set `NODE_CHANNEL_SERIALIZATION_MODE=json`.

### Linux and macOS (`parent.py`)

- The parent creates a Unix socket pair and passes one end to the child with `pass_fds`. `NODE_CHANNEL_FD` is set to that end's fd number.
- Each message is one line of JSON, ending in a newline.
- The child is started with `asyncio.create_subprocess_exec`, and the parent's end of the socket is used as an asyncio stream.

### Windows (`parent_windows.py`)

Windows needs more work, because Node expects the channel at C runtime file descriptor 3, and `subprocess` has no way to set that up.

- **Pipe:** the parent creates an overlapped named pipe. The child inherits the pipe's server end, and the parent connects to it as the client through asyncio's `ProactorEventLoop`.
- **fd 3:** the child is started with `CreateProcessW` through ctypes. Its `STARTUPINFO.lpReserved2` buffer is filled in so that the child's C runtime maps the pipe handle to fd 3.
- **Framing:** on Windows, libuv wraps every write in a 16-byte frame header, so messages are wrapped and unwrapped in that format.
- **Waiting:** a worker thread waits for the process to exit, so `wait()` can be awaited and still returns the exit code.

This needs the `ProactorEventLoop`, which is asyncio's default on Windows. `start()` raises `RuntimeError` with any other event loop.

## Limitations

- **JSON only:** messages must be JSON-serializable. Node's `advanced` serialization mode isn't supported.
- **No handle passing:** sending sockets or servers with `process.send(message, handle)` isn't supported.
- **64 KiB message limit on Linux and macOS:** a message from the child larger than 64 KiB makes `receive()` raise `ValueError`, because that's the default line length limit of asyncio streams. Windows has no such limit.

## Files

| File | Purpose |
|---|---|
| `parent.py` | Python parent for Linux and macOS |
| `parent_windows.py` | Python parent for Windows |
| `child.js` | Example child. It sends a `hello` message after 2 seconds, logs what it receives, and exits on `{"type": "exit"}` or when the channel closes. |
| `debug.js` | Diagnostic child. It prints the IPC environment it sees, then sends one test message, retrying if the send fails. Use it in place of `child.js` when the channel doesn't come up. |
| `parent.ts` | Node-to-Node reference that forks `child.js` with `child_process.fork()`. Run it with `npm install` then `npm run run`. It doesn't exit on its own, so stop it with Ctrl+C. |
| `test_ipc.py` | End-to-end test |

## Testing

```sh
uv run python test_ipc.py -v
```

The test starts `child.js` and waits for its `hello` message. It then sends some pings and an `exit` message, and checks that the child exits with code 0 and that the channel closes. The whole test, including cleanup, has a 15-second timeout.

CI (`.github/workflows/test.yml`) runs the test on Linux, macOS and Windows with Node 20, 22 and 24. A final `test-result` job passes only if every run passed, and merging into `main` requires that check.

## Linting and type checking

`uv sync` also installs the dev tools, [ruff](https://docs.astral.sh/ruff/) and [basedpyright](https://docs.basedpyright.com/), whose settings are in `pyproject.toml`.

```sh
uv run ruff check .
uv run basedpyright
```

Each parent module only type-checks on its own platform. To check the Windows module from Linux or macOS, run `uv run basedpyright --pythonplatform Windows parent_windows.py`.
