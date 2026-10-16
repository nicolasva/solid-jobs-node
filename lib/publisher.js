'use strict';

const crypto = require('node:crypto');
const Lab = require('./lab');
const keyspace = require('./keyspace');
const publicationBarrier = require('./publication_barrier');
const settings = require('./settings');
const { InvalidArgumentError, InvalidJobError, RedisError } = require('./errors');
const { emit } = require('./instrumentation');
const { dumpJson, isPlainObject, realtimeMilliseconds, randomHex } = require('./utilities');

const DEFAULT_CHUNK_SIZE = 1000;
const PLANNED_CHUNK_SIZE = 100;

function* chunks(items, size) {
  for (let start = 0; start < items.length; start += size) yield [start, items.slice(start, start + size)];
}

// Returns the first value that is not JSON-native (or has a non-string key), else undefined.
function invalidJsonValue(value) {
  if (value === null || ['string', 'boolean'].includes(typeof value)) return undefined;
  if (typeof value === 'number') return Number.isFinite(value) ? undefined : value;
  if (Array.isArray(value)) {
    for (const nested of value) {
      const invalid = invalidJsonValue(nested);
      if (invalid !== undefined) return invalid;
    }
    return undefined;
  }
  if (isPlainObject(value)) {
    for (const nested of Object.values(value)) {
      const invalid = invalidJsonValue(nested);
      if (invalid !== undefined) return invalid;
    }
    return undefined;
  }
  return value === undefined ? 'undefined' : value;
}

class Publisher {
  constructor(config) {
    this.config = config || settings.config();
  }

  async publish(envelope) {
    let normalized = this.normalize(envelope);
    normalized = await this.intercept(normalized);
    if (!normalized) return null;
    this.validateArguments(normalized.arguments);
    const barriers = await this.persist([normalized]);
    await this.emitPersisted([normalized], barriers);
    return normalized.id;
  }

  async publishMany(envelope) {
    if (!isPlainObject(envelope)) throw new InvalidJobError('Task envelope must be an object');
    const source = { ...envelope };
    const argumentSets = source.arguments;
    delete source.arguments;
    if (!Array.isArray(argumentSets)) throw new InvalidJobError("Task 'arguments' must be an array of argument arrays");
    const runTimes = source.run_at ?? null;
    delete source.run_at;
    if (!(runTimes === null || typeof runTimes === 'number' || Array.isArray(runTimes))) {
      throw new InvalidJobError("Task 'run_at' must be a number or an array");
    }
    if (Array.isArray(runTimes) && runTimes.length !== argumentSets.length) {
      throw new InvalidJobError("Task 'run_at' array must match the number of argument sets");
    }
    const chunkSize = Number(source.chunk_size || (runTimes !== null ? PLANNED_CHUNK_SIZE : DEFAULT_CHUNK_SIZE));
    delete source.chunk_size;
    if (!(chunkSize > 0)) throw new RangeError('chunkSize must be positive');
    const explicitId = source.id;
    delete source.id;
    if (explicitId && argumentSets.length > 1) throw new InvalidJobError('An explicit id is only valid for a single task');

    const template = this.normalize({ ...source, arguments: [] });
    delete template.id;
    const ids = [];
    for (const [offset, chunk] of chunks(argumentSets, chunkSize)) {
      const envelopes = [];
      for (const [index, args] of chunk.entries()) {
        if (!Array.isArray(args)) throw new InvalidJobError("Each task's arguments must be an array");
        const item = { ...template, arguments: [...args], id: explicitId || crypto.randomUUID() };
        if (runTimes !== null) item.run_at = Array.isArray(runTimes) ? runTimes[offset + index] : runTimes;
        const intercepted = await this.intercept(item);
        if (intercepted) {
          this.validateArguments(intercepted.arguments);
          envelopes.push(intercepted);
          ids.push(intercepted.id);
        } else ids.push(null);
      }
      const barriers = await this.persist(envelopes);
      await this.emitPersisted(envelopes, barriers);
    }
    return ids;
  }

  async persist(envelopes) {
    if (envelopes.length === 0) return [];
    const mode = Lab.mode();
    if (mode === 'capture') {
      for (const envelope of envelopes) Lab.capturedFor(envelope.task).push(envelope);
      return [];
    }
    if (mode === 'execute') {
      for (const envelope of envelopes) await Lab.execute(envelope);
      return [];
    }

    const planned = envelopes.filter((e) => e.run_at !== undefined && e.run_at !== null);
    const ready = envelopes.filter((e) => e.run_at === undefined || e.run_at === null);
    const pool = await this.config.connect();
    let barriers = [];
    await pool.pipelined((pipeline) => {
      for (const envelope of planned) {
        const score = envelope.run_at;
        delete envelope.run_at;
        pipeline.call('ZADD', keyspace.PLANNED, score, dumpJson(envelope));
      }
      barriers = this.persistReady(pipeline, ready);
    });
    return barriers;
  }

  async emitPersisted(envelopes, barriers) {
    for (const envelope of envelopes) {
      emit(this.config, 'enqueued', envelope);
      emit(this.config, 'journaled', envelope);
    }
    await this.releasePublications(barriers);
  }

  normalize(envelope) {
    if (!isPlainObject(envelope)) throw new InvalidJobError('Task envelope must be an object');
    const item = { ...this.config.defaultTaskOptions, ...stringifyKeysShallow(envelope) };
    const { task, arguments: args } = item;
    if (!((typeof task === 'function') || (typeof task === 'string' && task))) {
      throw new InvalidJobError('Task envelope requires a non-empty task');
    }
    if (!Array.isArray(args)) throw new InvalidJobError('Task arguments must be an array');
    item.arguments = [...args];
    if (typeof task === 'function') item.task = require('./task').taskNameOf(task);
    item.channel = String(item.channel);
    item.id = item.id || crypto.randomUUID();
    item.created_ms ??= realtimeMilliseconds();
    return item;
  }

  async intercept(envelope) {
    const context = { task: envelope.task, envelope, redisPool: this.config.pool };
    return this.config.publishInterceptors.call(context, () => envelope);
  }

  persistReady(pipeline, envelopes) {
    const barriers = [];
    const byChannel = new Map();
    for (const envelope of envelopes) {
      if (!byChannel.has(envelope.channel)) byChannel.set(envelope.channel, []);
      byChannel.get(envelope.channel).push(envelope);
    }
    for (const [channel, group] of byChannel) {
      pipeline.call('SADD', keyspace.CHANNELS, channel);
      const queuedMs = realtimeMilliseconds();
      const encoded = [];
      for (const envelope of group) {
        envelope.queued_ms = queuedMs;
        envelope[publicationBarrier.FIELD] = randomHex(16);
        encoded.push(dumpJson(envelope));
        const barrier = publicationBarrier.prepare({ publication: envelope[publicationBarrier.FIELD] });
        publicationBarrier.mark(pipeline, barrier);
        barriers.push(barrier);
      }
      pipeline.call('LPUSH', keyspace.channel(channel), ...encoded);
    }
    return barriers;
  }

  async releasePublications(barriers) {
    try {
      await publicationBarrier.release(await this.config.connect(), barriers);
    } catch (error) {
      if (!(error instanceof RedisError)) throw error;
      this.config.logger.warn(`SolidJobs publication barrier release failed; waiting for expiry: ${error.name}: ${error.message}`);
    }
  }

  validateArguments(args) {
    const invalid = invalidJsonValue(args);
    if (invalid === undefined) return;
    const message = `Task arguments must contain JSON-native values with plain object keys; found ${String(invalid)}`;
    const policy = this.config.onComplexArguments;
    if (policy === 'raise') throw new InvalidArgumentError(message);
    if (policy === 'warn') this.config.logger.warn(message);
  }
}

// Top-level keys only: argument payloads must stay untouched.
function stringifyKeysShallow(envelope) {
  return Object.fromEntries(Object.entries(envelope).map(([key, value]) => [String(key), value]));
}

module.exports = { Publisher, DEFAULT_CHUNK_SIZE, PLANNED_CHUNK_SIZE, invalidJsonValue };
