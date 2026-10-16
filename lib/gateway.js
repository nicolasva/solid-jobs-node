'use strict';

// Lifecycle of the Ruby sidecar that embeds the `solid-redis` gem.

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { GatewayError } = require('./errors');

const GATEWAY_SCRIPT = path.join(__dirname, 'solid_redis_gateway.rb');

class Sidecar {
  constructor(redisUrl, rubyCommand = ['ruby'], startupTimeoutMs = 15_000) {
    this.redisUrl = redisUrl;
    this.rubyCommand = rubyCommand;
    this.startupTimeoutMs = startupTimeoutMs;
    this.directory = null;
    this.socketPath = null;
    this.process = null;
  }

  async start() {
    // mkdtemp creates the directory 0700, the gateway binds the socket 0600.
    this.directory = fs.mkdtempSync(path.join(os.tmpdir(), 'solid_jobs_'));
    this.socketPath = path.join(this.directory, 'ipc.sock');
    const [executable, ...prefix] = this.rubyCommand;
    const child = spawn(executable, [...prefix, GATEWAY_SCRIPT, '--socket', this.socketPath,
      '--redis-url', this.redisUrl], { stdio: ['pipe', 'pipe', 'pipe'] });
    this.process = child;
    this.exited = false;
    let stderr = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk) => { stderr += chunk; });
    child.once('exit', () => { this.exited = true; });

    try {
      await new Promise((resolve, reject) => {
        let buffer = '';
        const timer = setTimeout(() => reject(new Error('timeout')), this.startupTimeoutMs);
        child.once('error', (error) => { clearTimeout(timer); reject(error); });
        child.once('exit', () => { clearTimeout(timer); reject(new Error('exited before READY')); });
        child.stdout.setEncoding('utf8');
        child.stdout.on('data', (chunk) => {
          buffer += chunk;
          if (buffer.split('\n').some((line) => line.trim() === 'READY')) {
            clearTimeout(timer);
            resolve();
          }
        });
      });
    } catch (error) {
      await this.stop();
      throw new GatewayError(`Ruby sidecar failed to start (${this.rubyCommand.join(' ')}): ${stderr.trim() || error.message}`);
    }

    // Detach the pipes from the event loop: only the sidecar's lifetime is managed explicitly.
    child.unref();
    for (const stream of [child.stdin, child.stdout, child.stderr]) stream.unref?.();
    const mode = fs.statSync(this.socketPath).mode & 0o777;
    if (mode & 0o077) {
      await this.stop();
      throw new GatewayError(`gateway socket permissions too open: 0o${mode.toString(8)}`);
    }
    return this;
  }

  alive() {
    return this.process !== null && !this.exited && this.process.exitCode === null;
  }

  async stop() {
    const child = this.process;
    this.process = null;
    if (child) {
      const exited = new Promise((resolve) => {
        if (child.exitCode !== null || child.signalCode !== null) resolve();
        else child.once('exit', resolve);
      });
      try { child.stdin.end(); } catch { /* already closed */ }
      const timer = setTimeout(() => child.kill('SIGKILL'), 3000);
      await exited;
      clearTimeout(timer);
    }
    this.cleanup();
  }

  stopSync() {
    const child = this.process;
    this.process = null;
    if (child) {
      try { child.kill('SIGKILL'); } catch { /* already gone */ }
    }
    this.cleanup();
  }

  cleanup() {
    if (this.directory) {
      fs.rmSync(this.directory, { recursive: true, force: true });
      this.directory = null;
    }
  }
}

const managed = new Map();

// Returns the socket path of a process-wide sidecar for `redisUrl`.
async function ensure(redisUrl, rubyCommand = ['ruby']) {
  const key = `${redisUrl}\u0000${rubyCommand.join(' ')}`;
  let entry = managed.get(key);
  if (entry && entry.sidecar && !entry.sidecar.alive()) {
    await entry.sidecar.stop();
    managed.delete(key);
    entry = undefined;
  }
  if (!entry) {
    const sidecar = new Sidecar(redisUrl, rubyCommand);
    entry = { sidecar, ready: sidecar.start().catch((error) => { managed.delete(key); throw error; }) };
    managed.set(key, entry);
  }
  await entry.ready;
  return entry.sidecar.socketPath;
}

async function shutdown() {
  const entries = [...managed.values()];
  managed.clear();
  await Promise.all(entries.map(async (entry) => {
    try { await entry.ready; } catch { return; }
    await entry.sidecar.stop();
  }));
}

process.on('exit', () => {
  for (const entry of managed.values()) entry.sidecar.stopSync();
});

module.exports = { Sidecar, ensure, shutdown, GATEWAY_SCRIPT };
