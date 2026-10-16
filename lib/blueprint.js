'use strict';

const os = require('node:os');
const path = require('node:path');
const gateway = require('./gateway');
const { ConfigurationError } = require('./errors');
const { NullInstrumenter } = require('./instrumentation');
const { InterceptorRegistry } = require('./interceptors');
const { GatewayPool } = require('./ipc');
const { randomHex } = require('./utilities');

const DEFAULT_TASK_OPTIONS = { channel: 'default', max_failures: 25 };
const CHANNEL_ORDERS = ['priority', 'weighted', 'shuffle'];
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };

function createLogger(level = 'warn') {
  const threshold = LEVELS[level] ?? LEVELS.warn;
  const write = (name) => (message) => {
    if (LEVELS[name] >= threshold) process.stderr.write(`${new Date().toISOString()} ${name.toUpperCase()} solid_jobs ${message}\n`);
  };
  return { level, debug: write('debug'), info: write('info'), warn: write('warn'), error: write('error') };
}

function normalizeChannels(values) {
  const list = typeof values === 'string' ? [values] : values;
  return list.map((value) => {
    let name;
    let weight = 1;
    if (Array.isArray(value)) [name, weight = 1] = value;
    else if (value && typeof value === 'object') ({ name, weight = 1 } = value);
    else if (typeof value === 'string' && value.includes(',')) {
      const [head, tail] = value.split(',');
      name = head;
      weight = tail;
    } else name = value;
    name = String(name);
    weight = Number(weight);
    if (!Number.isInteger(weight) || weight <= 0) throw new ConfigurationError('Channel weight must be a positive integer');
    return [name, weight];
  });
}

class Blueprint {
  constructor({ redisUrl, concurrency = 5, channels = ['default'], gatewaySocket } = {}) {
    this.redisUrl = redisUrl || process.env.REDIS_URL || 'redis://127.0.0.1:6379/0';
    this.gatewaySocket = gatewaySocket || process.env.SOLID_JOBS_GATEWAY_SOCKET || null;
    this.rubyCommand = (process.env.SOLID_JOBS_RUBY || 'ruby').split(/\s+/).filter(Boolean);
    this.concurrency = Number(concurrency);
    this.channelEntries = normalizeChannels(channels);
    this.defaultTaskOptions = { ...DEFAULT_TASK_OPTIONS };
    this.publishInterceptors = new InterceptorRegistry();
    this.executeInterceptors = new InterceptorRegistry();
    this.onComplexArguments = 'raise'; // 'raise' | 'warn' | 'ignore'
    this.pollIntervalAverage = 5.0;
    this.shutdownTimeout = 25.0;
    this.channelOrder = 'weighted';
    this.reliableFetch = true;
    this.retryBaseDelay = 15.0;
    this.retryMaxDelay = 3600.0;
    this.discardedLimit = 10_000;
    this.discardedRetention = 180 * 24 * 60 * 60;
    this.logger = createLogger(process.env.SOLID_JOBS_LOG_LEVEL || 'warn');
    this.identity = `${os.hostname()}:${process.pid}:${randomHex(6)}`;
    this.instrumenter = new NullInstrumenter();
    this.requires = [];
    this.callbacks = {};
    this.pool = null;
    this.sharedPool = false;
    this.gatewaySockets = null;
    // Ruby sidecar processes; null = automatic (min(concurrency, 4), 1 with warm IPC)
    this.gatewayProcesses = Number(process.env.SOLID_JOBS_GATEWAY_PROCESSES) > 0 ? Math.floor(Number(process.env.SOLID_JOBS_GATEWAY_PROCESSES)) : null;
    // Reserve the next job while the current one runs (a second, fully independent claim slot per worker)
    this.prefetch = ['1', 'true'].includes(process.env.SOLID_JOBS_PREFETCH);
    this.warmIpc = false; // multiplex all worker Redis traffic over the main thread's single connection
  }

  get channels() {
    return this.channelEntries.map(([name]) => name);
  }

  set channels(values) {
    this.channelEntries = normalizeChannels(values);
  }

  get channelOrder() {
    return this.order;
  }

  set channelOrder(value) {
    if (!CHANNEL_ORDERS.includes(value)) throw new ConfigurationError(`channelOrder must be one of ${CHANNEL_ORDERS.join(', ')}`);
    this.order = value;
  }

  sidecarCount() {
    if (this.gatewayProcesses) return this.gatewayProcesses;
    return this.warmIpc ? 1 : Math.min(Math.max(1, Number(this.concurrency) || 1), 4);
  }

  // Starts (once per process) the Ruby sidecar if needed and opens this thread's gateway connection.
  async connect() {
    if (this.pool) return this.pool;
    if (!this.gatewaySocket) {
      this.gatewaySockets = await gateway.ensureMany(this.redisUrl, this.rubyCommand, this.sidecarCount());
      [this.gatewaySocket] = this.gatewaySockets;
    }
    if (!this.pool) this.pool = new GatewayPool(this.gatewaySocket);
    return this.pool;
  }

  get redisPool() {
    if (this.pool) return this.pool;
    if (!this.gatewaySocket) throw new ConfigurationError('Redis is not connected: call `await config.connect()` first');
    this.pool = new GatewayPool(this.gatewaySocket);
    return this.pool;
  }

  on(event, callback) {
    (this.callbacks[event] ||= []).push(callback);
    return this;
  }

  async fire(event, ...args) {
    for (const callback of this.callbacks[event] || []) await callback(...args);
  }

  close() {
    if (this.pool) {
      if (!this.sharedPool) this.pool.close();
      this.pool = null;
    }
  }

  // Plain data that crosses to worker threads (functions and class instances cannot).
  snapshot() {
    return {
      redisUrl: this.redisUrl,
      gatewaySocket: this.gatewaySocket,
      gatewaySockets: this.gatewaySockets,
      rubyCommand: this.rubyCommand,
      concurrency: this.concurrency,
      channelEntries: this.channelEntries,
      defaultTaskOptions: this.defaultTaskOptions,
      onComplexArguments: this.onComplexArguments,
      pollIntervalAverage: this.pollIntervalAverage,
      shutdownTimeout: this.shutdownTimeout,
      channelOrder: this.channelOrder,
      reliableFetch: this.reliableFetch,
      retryBaseDelay: this.retryBaseDelay,
      retryMaxDelay: this.retryMaxDelay,
      discardedLimit: this.discardedLimit,
      discardedRetention: this.discardedRetention,
      identity: this.identity,
      warmIpc: this.warmIpc,
      gatewayProcesses: this.gatewayProcesses,
      prefetch: this.prefetch,
      logLevel: this.logger.level,
      requires: this.requires.map((entry) => resolveRequire(entry)),
    };
  }

  static fromSnapshot(snapshot) {
    const blueprint = new Blueprint({
      redisUrl: snapshot.redisUrl,
      concurrency: snapshot.concurrency,
      channels: snapshot.channelEntries,
      gatewaySocket: snapshot.gatewaySocket,
    });
    for (const key of ['rubyCommand', 'defaultTaskOptions', 'onComplexArguments', 'pollIntervalAverage',
      'shutdownTimeout', 'channelOrder', 'reliableFetch', 'retryBaseDelay', 'retryMaxDelay', 'discardedLimit',
      'discardedRetention', 'identity', 'requires', 'warmIpc', 'prefetch', 'gatewayProcesses', 'gatewaySockets']) {
      blueprint[key] = snapshot[key];
    }
    blueprint.logger = createLogger(snapshot.logLevel);
    return blueprint;
  }
}

// Worker threads resolve task files themselves, so hand them absolute paths (or package names as-is).
function resolveRequire(entry) {
  if (path.isAbsolute(entry)) return entry;
  if (entry.startsWith('.') || entry.endsWith('.js') || entry.endsWith('.cjs') || entry.endsWith('.mjs')) {
    return path.resolve(process.cwd(), entry);
  }
  try {
    return require.resolve(entry, { paths: [process.cwd()] });
  } catch {
    return entry;
  }
}

module.exports = { Blueprint, createLogger, normalizeChannels, resolveRequire, DEFAULT_TASK_OPTIONS };
