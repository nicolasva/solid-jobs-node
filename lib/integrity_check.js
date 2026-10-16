'use strict';

// Consistency audit: classifies every task id found in Redis and reports anomalies.
const keyspace = require('./keyspace');
const settings = require('./settings');
const { pairs, scan } = require('./utilities');

const ACTIVE_COLLECTIONS = [
  [keyspace.PLANNED, 'PLANNED'], [keyspace.RETRIES, 'RETRYING'], [keyspace.DISCARDED, 'DISCARDED'],
];

class Report {
  constructor(fields) {
    Object.assign(this, fields);
  }

  ok() {
    return !(this.lost.length || this.orphaned.length || this.invalidReservations.length
      || this.danglingIndexes.length || this.duplicates.length || this.malformedPayloads.length);
  }
}

class IntegrityCheck {
  constructor({ expectedJobIds, ackedJobIds = [], ackedKey = null, config } = {}) {
    this.config = config || settings.config();
    this.expected = new Set([...expectedJobIds].map(String));
    this.acked = new Set([...ackedJobIds].map(String));
    this.ackedKey = ackedKey;
    this.states = new Map();
    this.malformed = [];
    this.invalid = [];
  }

  static call(options) {
    return new IntegrityCheck(options).run();
  }

  async run() {
    this.redis = await this.config.connect();
    if (this.ackedKey) for (const id of (await this.redis.call('SMEMBERS', this.ackedKey)) || []) this.acked.add(id);
    await this.readChannels();
    await this.readTimedCollections();
    const claimed = await this.readClaimed();
    await this.validateClaimMetadata(claimed);
    for (const jobId of this.acked) this.addState(jobId, 'ACKED', 'acknowledged');
    const observed = new Set(this.states.keys());
    return new Report({
      lost: [...this.expected].filter((id) => !observed.has(id)).sort(),
      orphaned: [...observed].filter((id) => !this.expected.has(id)).sort(),
      invalidReservations: this.invalid,
      danglingIndexes: await this.danglingAttempts(),
      duplicates: [...this.states].filter(([, locations]) => locations.length > 1)
        .map(([jobId, locations]) => ({ jobId, locations })),
      malformedPayloads: this.malformed,
      states: Object.fromEntries(this.states),
    });
  }

  async readChannels() {
    for (const key of await scan(this.redis, `${keyspace.PREFIX}:channel:*`)) {
      const raws = (await this.redis.call('LRANGE', key, 0, -1)) || [];
      raws.forEach((raw, index) => this.addPayload(raw, 'READY', `${key}[${index}]`));
    }
  }

  async readTimedCollections() {
    for (const [key, state] of ACTIVE_COLLECTIONS) {
      const raws = (await this.redis.call('ZRANGE', key, 0, -1)) || [];
      raws.forEach((raw, index) => this.addPayload(raw, state, `${key}[${index}]`));
    }
  }

  async readClaimed() {
    const claimed = new Map();
    for (const key of await scan(this.redis, `${keyspace.PREFIX}:node:*:claimed:*`)) {
      const records = [];
      const raws = (await this.redis.call('LRANGE', key, 0, -1)) || [];
      raws.forEach((raw, index) => {
        const payload = this.parsePayload(raw, `${key}[${index}]`);
        if (payload) {
          this.addState(payload.id, 'CLAIMED', `${key}[${index}]`);
          records.push(payload);
        }
      });
      claimed.set(key, records);
    }
    return claimed;
  }

  async validateClaimMetadata(claimed) {
    const metadata = await this.claimMetadata();
    for (const [key, payloads] of claimed) {
      const [nodeKey, executorId] = key.split(':claimed:');
      const identity = nodeKey.slice(`${keyspace.PREFIX}:node:`.length);
      const mapKey = `${identity}\u0000${executorId}`;
      const entry = metadata.get(mapKey);
      metadata.delete(mapKey);
      if (payloads.length === 0) this.invalid.push({ key, reason: 'empty_claimed_list' });
      else if (payloads.length > 1) this.invalid.push({ key, reason: 'multiple_tasks_for_executor' });
      const payload = payloads[0] || null;
      if (!entry) {
        this.invalid.push({ key, reason: 'missing_metadata', jobId: payload ? payload.id : null });
        continue;
      }
      if (!payload) continue;
      const expected = { task_id: payload.id, node_id: identity, executor_id: executorId, channel: payload.channel };
      for (const [name, value] of Object.entries(expected)) {
        const actual = entry.attributes[name];
        if (String(actual) !== String(value)) this.invalid.push({ key, reason: 'metadata_mismatch', field: name, expected: value, actual });
      }
    }
    for (const entry of metadata.values()) {
      this.invalid.push({ key: entry.key, reason: 'metadata_without_payload', field: entry.field, jobId: entry.attributes.task_id });
    }
  }

  async claimMetadata() {
    const entries = new Map();
    for (const key of await scan(this.redis, `${keyspace.PREFIX}:node:*:claims`)) {
      const identity = key.slice(`${keyspace.PREFIX}:node:`.length, -':claims'.length);
      for (const [name, raw] of Object.entries(await this.hash(key))) {
        const attributes = this.parseJson(raw, `${key}[${name}]`);
        if (attributes) entries.set(`${identity}\u0000${name}`, { key, field: name, attributes });
      }
    }
    return entries;
  }

  async danglingAttempts() {
    const dangling = [];
    for (const [jobId, attempt] of Object.entries(await this.hash(keyspace.ATTEMPTS))) {
      if ((this.states.get(jobId) || []).some((location) => location.state !== 'ACKED')) continue;
      dangling.push({ jobId, attempt: Number(attempt), index: keyspace.ATTEMPTS });
    }
    return dangling;
  }

  addPayload(raw, state, location) {
    const payload = this.parsePayload(raw, location);
    if (payload) this.addState(payload.id, state, location);
  }

  parsePayload(raw, location) {
    const payload = this.parseJson(raw, location);
    if (payload === null) return null;
    if (payload && typeof payload === 'object' && payload.id) return payload;
    this.malformed.push({ location, error: 'missing id', raw });
    return null;
  }

  parseJson(raw, location) {
    try {
      return JSON.parse(raw);
    } catch (error) {
      this.malformed.push({ location, error: error.message, raw });
      return null;
    }
  }

  addState(jobId, state, location) {
    const key = String(jobId);
    if (!this.states.has(key)) this.states.set(key, []);
    this.states.get(key).push({ state, location });
  }

  async hash(key) {
    return Object.fromEntries(pairs(await this.redis.call('HGETALL', key)));
  }
}

module.exports = { IntegrityCheck, Report };
