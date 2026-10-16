'use strict';

// Warm IPC: the main thread owns the single persistent connection to the sidecar and multiplexes the Redis
// traffic of every worker thread over it. Workers talk to the main thread by postMessage, matched by request id;
// the claim/ack/fencing logic is unchanged, only the transport differs.
const { RedisCommandError, RedisConnectionError } = require('./errors');
const { Pipeline, toCommand } = require('./ipc');

const REQUEST_TIMEOUT_MS = 30_000;

class WarmIPCGateway {
  constructor(pool) {
    this.pool = pool;
    this.workers = new Set();
  }

  attach(worker) {
    this.workers.add(worker);
    const onMessage = (message) => {
      if (message && message.type === 'ipc') this.handle(worker, message);
    };
    worker.on('message', onMessage);
    worker.once('exit', () => this.workers.delete(worker));
  }

  async handle(worker, { id, payload }) {
    let reply;
    try {
      reply = { type: 'ipc-result', id, ok: true, data: await this.pool.send(payload) };
    } catch (error) {
      reply = { type: 'ipc-result', id, ok: false, error: { name: error.name, message: error.message } };
    }
    if (this.workers.has(worker)) worker.postMessage(reply);
  }
}

// Same interface as GatewayPool, but forwards every request to the main thread.
class ProxyPool {
  constructor(port, timeoutMs = REQUEST_TIMEOUT_MS) {
    this.port = port;
    this.timeoutMs = timeoutMs;
    this.pending = new Map();
    this.counter = 0;
    this.closed = false;
  }

  // Returns true when the message was a reply for this pool.
  receive(message) {
    if (!message || message.type !== 'ipc-result') return false;
    const entry = this.pending.get(message.id);
    if (!entry) return true;
    this.pending.delete(message.id);
    clearTimeout(entry.timer);
    if (message.ok) entry.resolve(message.data);
    else {
      const Klass = message.error.name === 'RedisConnectionError' ? RedisConnectionError : RedisCommandError;
      entry.reject(new Klass(message.error.message));
    }
    return true;
  }

  send(payload) {
    if (this.closed) return Promise.reject(new RedisConnectionError('Redis pool is closed'));
    return new Promise((resolve, reject) => {
      this.counter += 1;
      const id = `w${this.counter}`;
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new RedisConnectionError(`Warm IPC timeout for request ${id}`));
      }, this.timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.port.postMessage({ type: 'ipc', id, payload });
    });
  }

  async call(...command) {
    const parts = toCommand(command);
    return this.send({ command: parts[0], args: parts.slice(1) });
  }

  async pipelined(build) {
    const pipeline = new Pipeline();
    await build(pipeline);
    if (pipeline.commands.length === 0) return [];
    return this.send({ pipeline: pipeline.commands });
  }

  close() {
    this.closed = true;
    for (const entry of this.pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(new RedisConnectionError('Redis pool is closed'));
    }
    this.pending.clear();
  }
}

module.exports = { WarmIPCGateway, ProxyPool };
