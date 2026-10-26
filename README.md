# node-ipc

Spawn a Node.js script from Python and talk to it over Node's built-in IPC channel, the same one `child_process.fork()` sets up.

The child is a plain Node script: it uses `process.send()` and `process.on('message')` as if a Node parent had forked it. No extra npm packages and no stdin/stdout protocol are needed.

```python
import asyncio
from node_ipc import NodeIPCProcess

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

The `node_ipc` package exports one class, `NodeIPCProcess`, which works the same on every platform:

```python
from node_ipc import NodeIPCProcess
```

| Member | Description |
|---|---|
| `NodeIPCProcess(script_path, args=None)` | Set up a child that runs `node script_path *args`. Nothing is spawned until it is started. |
| `async with NodeIPCProcess(...) as proc` | Start the child on entry. On exit, terminate it if it is still running and close the channel. |
| `await proc.start()` / `await proc.close()` | The same steps as `async with`, without the context manager. Each instance runs one child, so starting it again raises `RuntimeError`, even after `close()`. |
| `await proc.send(message)` | Send a JSON-serializable dict. The child receives it in `process.on('message')`. |
| `await proc.receive()` | Wait for the next message the child sent with `process.send()`. Raises `EOFError` once the channel is closed. |
| `async for message in proc` | Iterate over incoming messages until the channel closes. |
| `await proc.wait()` | Wait for the child to exit and return its exit code. |

The child's stdout and stderr are inherited from the Python process.

## How it works

Node activates the IPC channel when it starts with `NODE_CHANNEL_FD` set to a file descriptor. Messages then go over that descriptor, and `NODE_CHANNEL_SERIALIZATION_MODE=json` makes each message one line of JSON, ending in a newline.

`NodeIPCProcess` handles the JSON lines on every platform. A backend creates the channel and moves bytes over it, and the package picks the backend for the current platform when it's imported.

### Linux and macOS (`node_ipc/_posix.py`)

- The parent creates a Unix socket pair and passes one end to the child with `pass_fds`. `NODE_CHANNEL_FD` is set to that end's fd number.
- The child is started with `asyncio.create_subprocess_exec`, and the parent's end of the socket is used as an asyncio stream.

### Windows (`node_ipc/_windows.py`)

Windows needs more work, because Node expects the channel at C runtime file descriptor 3, and `subprocess` has no way to set that up.

- **Pipe:** the parent creates an overlapped named pipe. The child inherits the pipe's server end, and the parent connects to it as the client through asyncio's `ProactorEventLoop`.
- **fd 3:** the child is started with `CreateProcessW` through ctypes. Its `STARTUPINFO.lpReserved2` buffer is filled in so that the child's C runtime maps the pipe handle to fd 3.
- **Framing:** on Windows, libuv wraps every write in a 16-byte frame header, so messages are wrapped and unwrapped in that format.
- **Waiting:** a worker thread waits for the process to exit, so `wait()` can be awaited and still returns the exit code.

This needs the `ProactorEventLoop`, which is asyncio's default on Windows. `start()` raises `RuntimeError` with any other event loop.

## Limitations

- **JSON only:** messages must be JSON-serializable. Node's `advanced` serialization mode isn't supported.
- **No handle passing:** sending sockets or servers with `process.send(message, handle)` isn't supported.

## Files

| File | Purpose |
|---|---|
| `node_ipc/__init__.py` | `NodeIPCProcess`: the shared API, message framing as JSON lines, and choosing the platform backend |
| `node_ipc/_posix.py` | Linux and macOS backend: spawning with a socket pair, raw bytes |
| `node_ipc/_windows.py` | Windows backend: named pipe, `CreateProcessW` with fd 3, libuv frames |
| `child.js` | Example child. It sends a `hello` message after 2 seconds, logs what it receives, sends back any `{"type": "echo"}` message, and exits on `{"type": "exit"}` or when the channel closes. |
| `debug.js` | Diagnostic child. It prints the IPC environment it sees, then sends one test message, retrying if the send fails. Use it in place of `child.js` when the channel doesn't come up. |
| `parent.ts` | Node-to-Node reference that forks `child.js` with `child_process.fork()`. Run it with `npm install` then `npm run run`. It doesn't exit on its own, so stop it with Ctrl+C. |
| `test_ipc.py` | End-to-end tests |

## Testing

```sh
uv run python test_ipc.py -v
```

The tests run against `child.js`:

- **Round trip:** waits for the child's `hello` message, then sends some pings and an `exit` message, and checks that the child exits with code 0 and that the channel closes.
- **Large message:** sends a 1 MB message and checks that the child echoes it back intact.

Each test, including cleanup, has a 15-second timeout.

CI (`.github/workflows/test.yml`) runs the tests on Linux, macOS and Windows with Node 20, 22 and 24. A final `test-result` job passes only if every run passed, and merging into `main` requires that check.

## Linting and type checking

`uv sync` also installs the dev tools, [ruff](https://docs.astral.sh/ruff/) and [basedpyright](https://docs.basedpyright.com/), whose settings are in `pyproject.toml`.

```sh
uv run ruff check .
uv run basedpyright
```

To type-check the Windows backend from Linux or macOS, run `uv run basedpyright --pythonplatform Windows`. Each backend starts with a `sys.platform` assertion, so code for the other platform is skipped instead of reported as errors.
