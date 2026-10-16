'use strict';

const keyspace = require('./keyspace');
const publicationBarrier = require('./publication_barrier');
const { RedisCommandError } = require('./errors');
const { emit, workerAttributes } = require('./instrumentation');
const { randomHex, runScript, sha1, sleep } = require('./utilities');

const TIMEOUT_MS = 250;
const IDLE_MIN_MS = 5;
const FIELD = publicationBarrier.FIELD;

const RESERVE = `
local payload = redis.call("rpop", KEYS[1])
if not payload then
  return nil
end
local job = cjson.decode(payload)
local publication = job["${FIELD}"]
if publication and redis.call("exists", ARGV[5] .. publication) == 1 then
  redis.call("rpush", KEYS[1], payload)
  return nil
end

redis.call("lpush", KEYS[2], payload)
local attempt = redis.call("hincrby", KEYS[4], job["id"], 1)
local metadata = {
  task_id = job["id"],
  claim_token = ARGV[1],
  node_id = ARGV[2],
  executor_id = ARGV[3],
  channel = job["channel"],
  claimed_at = tonumber(ARGV[4]),
  attempt = attempt
}
redis.call("hset", KEYS[3], ARGV[3], cjson.encode(metadata))
return {payload, attempt}
`;

const POP_READY = `
local payload = redis.call("rpop", KEYS[1])
if not payload then
  return nil
end
local job = cjson.decode(payload)
local publication = job["${FIELD}"]
if publication and redis.call("exists", ARGV[1] .. publication) == 1 then
  redis.call("rpush", KEYS[1], payload)
  return nil
end
return payload
`;

const REGISTER = `
local attempt = redis.call("hincrby", KEYS[2], ARGV[1], 1)
local metadata = cjson.decode(ARGV[3])
metadata["attempt"] = attempt
redis.call("hset", KEYS[1], ARGV[2], cjson.encode(metadata))
return attempt
`;

const ACK = `
local metadata = redis.call("hget", KEYS[2], ARGV[2])
if not metadata then
  return 0
end
if cjson.decode(metadata)["claim_token"] ~= ARGV[3] then
  return 0
end
local removed = redis.call("lrem", KEYS[1], 1, ARGV[1])
redis.call("hdel", KEYS[2], ARGV[2])
if removed > 0 then
  redis.call("hdel", KEYS[3], ARGV[4])
end
return removed
`;

const REQUEUE = `
local metadata = redis.call("hget", KEYS[2], ARGV[2])
if not metadata then
  return 0
end
if cjson.decode(metadata)["claim_token"] ~= ARGV[3] then
  return 0
end
local removed = redis.call("lrem", KEYS[1], 1, ARGV[1])
if removed > 0 then
  redis.call("rpush", KEYS[3], ARGV[1])
end
redis.call("hdel", KEYS[2], ARGV[2])
return removed
`;

const [RESERVE_SHA, POP_READY_SHA, REGISTER_SHA, ACK_SHA, REQUEUE_SHA] = [RESERVE, POP_READY, REGISTER, ACK, REQUEUE].map(sha1);

function shuffled(list) {
  const copy = [...list];
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

const unique = (list) => [...new Set(list)];

class ClaimRecord {
  constructor(fields) {
    Object.assign(this, fields);
  }

  ackArguments() {
    return [[this.claimedKey, this.claimsKey, keyspace.ATTEMPTS],
      [this.payload, this.executorField, this.claimToken, this.envelope.id]];
  }

  async complete() {
    if (!this.claimedKey) return true;
    const [keys, args] = this.ackArguments();
    return Number(await runScript(this.redisPool, ACK, ACK_SHA, keys, ...args)) === 1;
  }

  async requeue() {
    const destination = keyspace.channel(this.channel);
    if (this.claimedKey) {
      const removed = await runScript(this.redisPool, REQUEUE, REQUEUE_SHA,
        [this.claimedKey, this.claimsKey, destination], this.payload, this.executorField, this.claimToken);
      return Number(removed) === 1;
    }
    await this.redisPool.call('RPUSH', destination, this.payload);
    return true;
  }
}

class Claim {
  constructor(config, { identity = null, processorId = null } = {}) {
    this.config = config;
    this.redisPool = config.redisPool;
    this.channelOrder = config.channelOrder;
    this.channels = config.channelEntries.flatMap(([name, weight]) => Array(weight).fill(keyspace.channel(name)));
    this.channelNames = new Map(this.channels.map((key) => [key, key.slice(keyspace.CHANNEL_PREFIX.length)]));
    this.availableChannels = this.channels;
    this.claimedKey = config.reliableFetch && identity ? keyspace.claimed(identity, processorId) : null;
    this.claimsKey = this.claimedKey ? keyspace.claims(identity) : null;
    this.identity = identity;
    this.processorId = processorId;
    this.executorField = String(processorId);
    this.pausedRefreshAt = 0;
    this.channelIndex = 0;
    this.checkClaimed = Boolean(this.claimedKey);
    this.pending = null;
    this.scriptsLoaded = false;
  }

  // Holds the acknowledgement so the next reservation sends it in the same Redis round trip. The ack still goes
  // out before anything else the worker does, so fencing is unchanged; only the extra round trip disappears.
  async deferAck(record, onResult) {
    if (!record.claimedKey) {
      onResult(true);
      return;
    }
    await this.flush();
    this.pending = [record, onResult];
  }

  async flush() {
    const pending = this.pending;
    this.pending = null;
    if (!pending) return;
    const [record, onResult] = pending;
    let acknowledged;
    try {
      acknowledged = await record.complete();
    } catch (error) {
      this.connectionFailed();
      throw error;
    }
    onResult(acknowledged);
  }

  async next() {
    if (this.checkClaimed) {
      await this.flush();
      this.checkClaimed = false;
      const payload = await this.redisPool.call('LINDEX', this.claimedKey, -1);
      if (payload) return this.existingClaim(payload);
    }

    await this.refreshPaused();
    const channels = this.availableChannels;
    if (channels.length === 0) {
      await this.flush();
      await sleep(50);
      return null;
    }
    return this.claimedKey ? this.claimReliable(channels) : this.claimUnreliable(channels);
  }

  connectionFailed() {
    this.checkClaimed = Boolean(this.claimedKey);
  }

  async record(payload, channel = null, claimToken = null, attempt = null) {
    const envelope = publicationBarrier.strip(JSON.parse(payload));
    const name = channel || envelope.channel;
    if (claimToken === null) [claimToken, attempt] = await this.register(envelope, name);
    const record = new ClaimRecord({
      channel: name, payload, envelope, redisPool: this.redisPool, claimedKey: this.claimedKey,
      claimsKey: this.claimsKey, executorField: this.executorField, claimToken, attempt, claims: this,
    });
    emit(this.config, 'reserved', envelope, { reservation_id: claimToken, attempt, ...workerAttributes(this.processorId) });
    return record;
  }

  async existingClaim(payload) {
    const raw = await this.redisPool.call('HGET', this.claimsKey, String(this.processorId));
    if (raw) {
      const metadata = JSON.parse(raw);
      const envelope = publicationBarrier.strip(JSON.parse(payload));
      if (metadata.task_id === envelope.id) {
        return new ClaimRecord({
          channel: envelope.channel, payload, envelope, redisPool: this.redisPool, claimedKey: this.claimedKey,
          claimsKey: this.claimsKey, executorField: this.executorField,
          claimToken: metadata.claim_token, attempt: Number(metadata.attempt), claims: this,
        });
      }
    }
    return this.record(payload);
  }

  async register(envelope, channel) {
    if (!this.claimedKey) return [null, null];
    const claimToken = randomHex(16);
    const metadata = JSON.stringify({
      task_id: envelope.id, claim_token: claimToken, node_id: this.identity,
      executor_id: this.processorId, channel, claimed_at: Date.now() / 1000,
    });
    const attempt = await runScript(this.redisPool, REGISTER, REGISTER_SHA,
      [this.claimsKey, keyspace.ATTEMPTS], envelope.id, this.processorId, metadata);
    return [claimToken, Number(attempt)];
  }

  nextChannel(channels) {
    if (this.channelOrder === 'shuffle') return channels[Math.floor(Math.random() * channels.length)];
    const channel = channels[this.channelIndex % channels.length];
    this.channelIndex += 1;
    return channel;
  }

  orderedSources(channels) {
    if (this.channelOrder === 'priority') return unique(channels);
    if (this.channelOrder === 'shuffle') return unique(shuffled(channels));
    return unique([this.nextChannel(channels), ...channels]);
  }

  async claimReliable(channels) {
    for (const source of this.orderedSources(channels)) {
      const claim = await this.claimNow(source);
      if (claim) return this.busy(claim);
    }
    await this.idleSleep();
    return null;
  }

  async claimUnreliable(channels) {
    for (const source of this.orderedSources(channels)) {
      const payload = await runScript(this.redisPool, POP_READY, POP_READY_SHA, [source], keyspace.PUBLICATION_PREFIX);
      if (payload) return this.busy(this.record(payload, this.channelNames.get(source)));
    }
    await this.idleSleep();
    return null;
  }

  busy(claim) {
    this.idleMs = IDLE_MIN_MS;
    return claim;
  }

  // Empty queues are polled quickly right after activity, backing off to TIMEOUT_MS when truly idle.
  async idleSleep() {
    const delay = this.idleMs || IDLE_MIN_MS;
    this.idleMs = Math.min(TIMEOUT_MS, delay * 2);
    await sleep(Math.min(TIMEOUT_MS, delay * (0.75 + Math.random() * 0.5)));
  }

  async claimNow(source) {
    const claimToken = randomHex(16);
    const keys = [source, this.claimedKey, this.claimsKey, keyspace.ATTEMPTS];
    const args = [claimToken, this.identity, this.processorId, Date.now() / 1000, keyspace.PUBLICATION_PREFIX];
    const result = this.pending
      ? await this.ackAndReserve(keys, args)
      : await runScript(this.redisPool, RESERVE, RESERVE_SHA, keys, ...args);
    if (!result) return null;
    const [payload, attempt] = result;
    return this.record(payload, this.channelNames.get(source), claimToken, Number(attempt));
  }

  async ackAndReserve(keys, args) {
    const [record, onResult] = this.pending;
    if (!this.scriptsLoaded) {
      await this.redisPool.call('SCRIPT', 'LOAD', ACK);
      await this.redisPool.call('SCRIPT', 'LOAD', RESERVE);
      this.scriptsLoaded = true;
    }
    const [ackKeys, ackArgs] = record.ackArguments();
    let replies;
    try {
      replies = await this.redisPool.pipelined((pipeline) => {
        pipeline.call('EVALSHA', ACK_SHA, ackKeys.length, ...ackKeys, ...ackArgs);
        pipeline.call('EVALSHA', RESERVE_SHA, keys.length, ...keys, ...args);
      });
    } catch (error) {
      if (!(error instanceof RedisCommandError) || !String(error.message).includes('NOSCRIPT')) throw error;
      // Scripts were flushed: reload, then let the ack run alone. The reservation inside the failed pipeline may
      // have succeeded, so re-adopt whatever sits in the claimed list.
      this.scriptsLoaded = false;
      this.checkClaimed = true;
      await this.flush();
      return null;
    }
    this.pending = null;
    const [removed, result] = replies;
    onResult(Number(removed) === 1);
    return result;
  }

  async refreshPaused() {
    const now = performance.now();
    if (now < this.pausedRefreshAt) return;
    const paused = (await this.redisPool.call('SMEMBERS', keyspace.PAUSED_CHANNELS)) || [];
    this.availableChannels = paused.length === 0
      ? this.channels
      : this.channels.filter((key) => !paused.includes(this.channelNames.get(key)));
    this.pausedRefreshAt = now + 5000;
  }
}

module.exports = { Claim, ClaimRecord, RESERVE, POP_READY, REGISTER, ACK, REQUEUE, TIMEOUT_MS };
