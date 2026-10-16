'use strict';

// Client side of the Unix-socket protocol spoken by solid_redis_gateway.rb.
// Node never speaks RESP: commands go out as JSON lines to the Ruby sidecar, which executes them through the
// `solid-redis` gem. Each pool owns one socket (one Redis connection inside the sidecar); requests are written
// immediately and replies are matched in order, so concurrent callers stay correct.

const net = require('node:net');
const { RedisCommandError, RedisConnectionError } = require('./errors');

const CONNECTION_ERRORS = new Set(['ConnectionError', 'TimeoutError', 'FailoverError', 'CheckoutTimeoutError']);

function toArgument(value) {
  if (typeof value === 'boolean') throw new TypeError('Redis arguments cannot be booleans');
  if (value === null || value === undefined) throw new TypeError('Redis arguments cannot be null or undefined');
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  return String(value);
}

function toCommand(command) {
  if (!command || command.length === 0) throw new TypeError('empty Redis command');
  return command.map(toArgument);
}

class Connection {
  constructor(socket, timeoutMs) {
    this.socket = socket;
    this.timeoutMs = timeoutMs;
    this.pending = [];
    this.chunks = [];
    this.closed = false;
    socket.setEncoding('utf8');
    socket.on('data', (chunk) => this.onData(chunk));
    socket.on('error', (error) => this.fail(error));
    socket.on('close', () => this.fail(new Error('gateway closed the connection')));
    socket.unref();
  }

  static connect(path, timeoutMs) {
    return new Promise((resolve, reject) => {
      const socket = net.connect({ path });
      const onError = (error) => reject(error);
      socket.once('error', onError);
      socket.once('connect', () => {
        socket.removeListener('error', onError);
        resolve(new Connection(socket, timeoutMs));
      });
    });
  }

  request(payload) {
    return new Promise((resolve, reject) => {
      if (this.closed) {
        reject(new Error('gateway connection is closed'));
        return;
      }
      const entry = { resolve, reject, timer: null };
      entry.timer = setTimeout(() => this.fail(new Error(`gateway request timed out after ${this.timeoutMs}ms`)),
        this.timeoutMs);
      this.pending.push(entry);
      if (this.pending.length === 1) this.socket.ref();
      this.socket.write(`${JSON.stringify(payload)}\n`);
    });
  }

  onData(chunk) {
    let newline = chunk.indexOf('\n');
    if (newline === -1) {
      this.chunks.push(chunk);
      return;
    }
    let rest = chunk;
    while (newline !== -1) {
      const line = this.chunks.join('') + rest.slice(0, newline);
      this.chunks = [];
      rest = rest.slice(newline + 1);
      this.reply(line);
      newline = rest.indexOf('\n');
    }
    if (rest) this.chunks.push(rest);
  }

  reply(line) {
    const entry = this.pending.shift();
    if (!entry) return;
    clearTimeout(entry.timer);
    if (this.pending.length === 0 && !this.closed) this.socket.unref();
    try {
      entry.resolve(JSON.parse(line));
    } catch (error) {
      entry.reject(error);
    }
  }

  fail(error) {
    if (this.closed) return;
    this.closed = true;
    const pending = this.pending;
    this.pending = [];
    for (const entry of pending) {
      clearTimeout(entry.timer);
      entry.reject(error);
    }
    this.socket.destroy();
  }

  close() {
    this.fail(new Error('connection closed'));
  }
}

class Pipeline {
  constructor() {
    this.commands = [];
  }

  call(...command) {
    this.commands.push(toCommand(command));
    return this;
  }
}

class GatewayPool {
  constructor(socketPath, timeoutMs = 30_000) {
    this.socketPath = socketPath;
    this.timeoutMs = timeoutMs;
    this.connection = null;
    this.connecting = null;
    this.closed = false;
  }

  async call(...command) {
    const parts = toCommand(command);
    return this.send({ command: parts[0], args: parts.slice(1) });
  }

  // Runs `build(pipeline)` then sends all collected commands in one round trip; resolves to the replies.
  async pipelined(build) {
    const pipeline = new Pipeline();
    await build(pipeline);
    if (pipeline.commands.length === 0) return [];
    return this.send({ pipeline: pipeline.commands });
  }

  close() {
    this.closed = true;
    const connection = this.connection;
    this.connection = null;
    this.connecting = null;
    if (connection) connection.close();
  }

  async getConnection() {
    if (this.connection && !this.connection.closed) return this.connection;
    if (this.closed) throw new RedisConnectionError('Redis pool is closed');
    if (!this.connecting) {
      this.connecting = Connection.connect(this.socketPath, this.timeoutMs).then(
        (connection) => {
          this.connection = connection;
          this.connecting = null;
          return connection;
        },
        (error) => {
          this.connecting = null;
          throw new RedisConnectionError(`cannot reach gateway at ${this.socketPath}: ${error.message}`);
        });
    }
    return this.connecting;
  }

  async send(payload) {
    const connection = await this.getConnection();
    let reply;
    try {
      reply = await connection.request(payload);
    } catch (error) {
      connection.close();
      if (this.connection === connection) this.connection = null;
      throw new RedisConnectionError(`gateway request failed: ${error.message}`);
    }
    if (reply.status === 'OK') return reply.data === undefined ? null : reply.data;
    const details = reply.error || {};
    const message = details.message || 'unknown gateway error';
    if (CONNECTION_ERRORS.has(details.class)) throw new RedisConnectionError(message);
    throw new RedisCommandError(message);
  }
}

module.exports = { GatewayPool, Pipeline, toCommand };
