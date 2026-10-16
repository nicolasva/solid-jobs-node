'use strict';

// Read/inspect API over the SolidJobs Redis keyspace (counters, channels, nodes, claims...). All methods are async.
const keyspace = require('./keyspace');
const settings = require('./settings');
const { Publisher } = require('./publisher');
const { pairs, scan } = require('./utilities');

const pool = (config) => (config || settings.config()).connect();
const toDate = (seconds) => (seconds === null || seconds === undefined ? null : new Date(Number(seconds) * 1000));

class Counters {
  constructor(config) {
    this.config = config || settings.config();
  }

  async completed() {
    return Number((await (await pool(this.config)).call('GET', keyspace.PROCESSED)) || 0);
  }

  async failed() {
    return Number((await (await pool(this.config)).call('GET', keyspace.FAILED)) || 0);
  }

  async ready() {
    let total = 0;
    for (const channel of await Channel.catalog(this.config)) total += await channel.size();
    return total;
  }

  async planned() {
    return Number(await (await pool(this.config)).call('ZCARD', keyspace.PLANNED));
  }

  async retrying() {
    return Number(await (await pool(this.config)).call('ZCARD', keyspace.RETRIES));
  }

  async discarded() {
    return Number(await (await pool(this.config)).call('ZCARD', keyspace.DISCARDED));
  }

  async nodes() {
    return Number(await (await pool(this.config)).call('SCARD', keyspace.NODES));
  }

  async executions() {
    let total = 0;
    for (const node of await Nodes.list(this.config)) total += node.activeCount();
    return total;
  }

  async reset() {
    await (await pool(this.config)).call('DEL', keyspace.PROCESSED, keyspace.FAILED);
    return this;
  }
}

class StoredTask {
  constructor(raw, { score = null, config, owner = null } = {}) {
    this.raw = raw;
    this.envelope = JSON.parse(raw);
    this.score = score === null ? null : Number(score);
    this.config = config || settings.config();
    this.owner = owner;
  }

  get id() { return this.envelope.id; }

  get channel() { return this.envelope.channel; }

  get task() { return this.envelope.task; }

  get arguments() { return this.envelope.arguments; }

  get createdAt() { return this.millis('created_ms'); }

  get queuedAt() { return this.millis('queued_ms'); }

  get dueAt() { return toDate(this.score); }

  async remove() {
    const redis = await pool(this.config);
    if (this.owner instanceof TimedTasks) return redis.call('ZREM', this.owner.key, this.raw);
    if (this.owner instanceof Channel) return redis.call('LREM', this.owner.storageKey, 1, this.raw);
    return false;
  }

  async requeue() {
    await this.remove();
    return new Publisher(this.config).publish(this.envelope);
  }

  millis(field) {
    const value = this.envelope[field];
    return value ? toDate(Number(value) / 1000) : null;
  }
}

class Channel {
  constructor(name, config) {
    this.name = String(name);
    this.config = config || settings.config();
  }

  static async catalog(config) {
    const cfg = config || settings.config();
    const names = (await (await pool(cfg)).call('SMEMBERS', keyspace.CHANNELS)) || [];
    return names.sort().map((name) => new Channel(name, cfg));
  }

  get storageKey() {
    return keyspace.channel(this.name);
  }

  async tasks() {
    const raws = (await (await pool(this.config)).call('LRANGE', this.storageKey, 0, -1)) || [];
    return raws.reverse().map((raw) => new StoredTask(raw, { config: this.config, owner: this }));
  }

  async size() {
    return Number(await (await pool(this.config)).call('LLEN', this.storageKey));
  }

  async waitingTime() {
    const raw = await (await pool(this.config)).call('LINDEX', this.storageKey, -1);
    if (!raw) return 0;
    const envelope = JSON.parse(raw);
    const stamp = envelope.queued_ms || envelope.created_ms;
    return stamp ? Math.max(Date.now() / 1000 - Number(stamp) / 1000, 0) : 0;
  }

  async purge() {
    await (await pool(this.config)).pipelined((pipeline) => {
      pipeline.call('DEL', this.storageKey);
      pipeline.call('SREM', keyspace.CHANNELS, this.name);
    });
    return this;
  }

  async paused() {
    return Number(await (await pool(this.config)).call('SISMEMBER', keyspace.PAUSED_CHANNELS, this.name)) === 1;
  }

  async pause() {
    await (await pool(this.config)).call('SADD', keyspace.PAUSED_CHANNELS, this.name);
    return this;
  }

  async resume() {
    await (await pool(this.config)).call('SREM', keyspace.PAUSED_CHANNELS, this.name);
    return this;
  }
}

class TimedTasks {
  constructor(key, config) {
    this.key = key;
    this.config = config || settings.config();
  }

  async tasks() {
    const values = await (await pool(this.config)).call('ZRANGE', this.key, 0, -1, 'WITHSCORES');
    return pairs(values).map(([raw, score]) => new StoredTask(raw, { score, config: this.config, owner: this }));
  }

  async size() {
    return Number(await (await pool(this.config)).call('ZCARD', this.key));
  }

  async purge() {
    await (await pool(this.config)).call('DEL', this.key);
    return this;
  }

  async locate(...ids) {
    const wanted = new Set(ids.flat());
    return (await this.tasks()).filter((task) => wanted.has(task.id));
  }

  async requeueAll() {
    for (const task of await this.tasks()) await task.requeue();
  }
}

class PlannedTasks extends TimedTasks {
  constructor(config) { super(keyspace.PLANNED, config); }
}

class RetryingTasks extends TimedTasks {
  constructor(config) { super(keyspace.RETRIES, config); }
}

class DiscardedTasks extends TimedTasks {
  constructor(config) { super(keyspace.DISCARDED, config); }
}

class Execution {
  constructor(nodeId, executorId, raw) {
    this.nodeId = nodeId;
    this.executorId = executorId;
    this.attributes = JSON.parse(raw);
  }

  get channel() { return this.attributes.channel; }

  get envelope() { return this.attributes.envelope; }

  get claimToken() { return this.attributes.claim_token; }

  get startedAt() { return toDate(this.attributes.started_at); }

  get attempt() { return Number(this.attributes.attempt || 1); }
}

const CONTROL_SIGNALS = { pause: 'TSTP', shutdown: 'TERM', backtraces: 'TTIN' };

class Node {
  constructor(identity, attributes, config) {
    this.identity = identity;
    this.config = config || settings.config();
    this.attributes = attributes;
    this.info = JSON.parse(attributes.info || '{}');
  }

  static async load(identity, config) {
    const flat = await (await pool(config)).call('HGETALL', keyspace.node(identity));
    return new Node(identity, Object.fromEntries(pairs(flat)), config);
  }

  get hostname() { return this.info.hostname; }

  get pid() { return Number(this.info.pid || 0); }

  get channels() { return this.info.channels || []; }

  activeCount() { return Number(this.attributes.busy || 0); }

  concurrency() { return Number(this.attributes.concurrency || 0); }

  quiet() { return this.attributes.quiet === 'true'; }

  lastSeenAt() { return 'beat' in this.attributes ? toDate(this.attributes.beat) : null; }

  async requestControl(action) {
    const signal = CONTROL_SIGNALS[action];
    if (!signal) throw new RangeError(`Unknown control action ${action}`);
    await (await pool(this.config)).call('LPUSH', keyspace.nodeSignals(this.identity), signal);
    return true;
  }

  async executions() {
    const fields = await (await pool(this.config)).call('HGETALL', keyspace.nodeWork(this.identity));
    return pairs(fields).map(([executor, raw]) => new Execution(this.identity, executor, raw));
  }
}

class Nodes {
  static async list(config) {
    const redis = await pool(config);
    const nodes = [];
    for (const identity of (await redis.call('SMEMBERS', keyspace.NODES)) || []) {
      const node = await Node.load(identity, config);
      if (node.lastSeenAt() !== null) nodes.push(node);
      else await redis.call('SREM', keyspace.NODES, identity);
    }
    return nodes;
  }
}

class ClaimInfo {
  constructor(raw) {
    this.attributes = JSON.parse(raw);
  }

  get taskId() { return this.attributes.task_id; }

  get claimToken() { return this.attributes.claim_token; }

  get nodeId() { return this.attributes.node_id; }

  get executorId() { return this.attributes.executor_id; }

  get channel() { return this.attributes.channel; }

  get attempt() { return Number(this.attributes.attempt); }

  get claimedAt() { return toDate(this.attributes.claimed_at); }
}

class Claims {
  static async list(config) {
    const redis = await pool(config);
    const result = [];
    for (const key of await scan(redis, `${keyspace.PREFIX}:node:*:claims`)) {
      for (const raw of (await redis.call('HVALS', key)) || []) result.push(new ClaimInfo(raw));
    }
    return result;
  }

  static async findTask(taskId, config) {
    return (await Claims.list(config)).find((claim) => claim.taskId === taskId) || null;
  }
}

class Executions {
  static async list(config) {
    const all = [];
    for (const node of await Nodes.list(config)) all.push(...(await node.executions()));
    return all;
  }
}

module.exports = {
  Counters, StoredTask, Channel, TimedTasks, PlannedTasks, RetryingTasks, DiscardedTasks,
  Execution, Node, Nodes, ClaimInfo, Claims, Executions,
};
