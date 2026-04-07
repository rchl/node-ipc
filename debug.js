/**
 * Run this instead of child.js to diagnose what Node sees during IPC bootstrap.
 * It defers process.send() until after the event loop ticks, giving the pipe
 * time to stabilise, and logs every relevant detail.
 */

import fs from 'fs';

console.error('[diag] NODE_CHANNEL_FD  =', process.env.NODE_CHANNEL_FD);
console.error('[diag] process.send     =', typeof process.send);
console.error('[diag] process.channel  =', process.channel);
console.error('[diag] pid              =', process.pid);

// Try to stat the fd to confirm it's alive
const fd = parseInt(process.env.NODE_CHANNEL_FD, 10);
try {
  const stat = fs.fstatSync(isNaN(fd) ? 3 : fd);
  console.error('[diag] fstat            =', JSON.stringify({
    isFIFO: stat.isFIFO(),
    isSocket: stat.isSocket(),
    isFile: stat.isFile(),
    size: stat.size,
  }));
} catch (e) {
  console.error('[diag] fstat error      =', e.message);
}

if (typeof process.send !== 'function') {
  console.error('[diag] FATAL: process.send is not a function — IPC channel not set up');
  process.exit(1);
}

// Defer send by several event loop ticks
let attempt = 0;
function trySend() {
  attempt++;
  console.error(`[diag] send attempt #${attempt}`);
  try {
    process.send({ type: 'diag', attempt, pid: process.pid });
    console.error('[diag] send succeeded');
  } catch (e) {
    console.error('[diag] send error =', e.message, e.code);
    if (attempt < 5) setTimeout(trySend, 200);
    else process.exit(1);
  }
}

// Wait 500ms before first attempt — gives Python time to be ready
setTimeout(trySend, 500);

process.on('message', (msg) => {
  console.error('[diag] received =', JSON.stringify(msg));
});

process.on('disconnect', () => {
  console.error('[diag] disconnected');
  process.exit(0);
});
