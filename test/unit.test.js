'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { Blueprint, normalizeChannels } = require('../lib/blueprint');
const { InterceptorRegistry } = require('../lib/interceptors');
const { GatewayPool } = require('../lib/ipc');
const { snakeCase, stringifyKeys, sha1 } = require('../lib/utilities');
const { ConfigurationError, RedisConnectionError } = require('../lib/errors');
const { registerExports, register, Task, taskNameOf } = require('../lib/task');
const { InvalidJobError } = require('../lib/errors');
const publicationBarrier = require('../lib/publication_barrier');
const { RESERVE, ACK } = require('../lib/claim');

test('normalizeChannels validates weights', () => {
  assert.deepEqual(normalizeChannels(['a,2', ['b', 3], { name: 'c' }, 'd']), [['a', 2], ['b', 3], ['c', 1], ['d', 1]]);
  assert.throws(() => normalizeChannels(['a,0']), ConfigurationError);
});

test('blueprint rejects an unknown channel order and snapshots round trip', () => {
  const config = new Blueprint({ concurrency: 3, channels: ['x,2'] });
  assert.throws(() => { config.channelOrder = 'nope'; }, ConfigurationError);
  const copy = Blueprint.fromSnapshot(JSON.parse(JSON.stringify(config.snapshot())));
  assert.equal(copy.identity, config.identity);
  assert.deepEqual(copy.channelEntries, [['x', 2]]);
});

test('interceptors run in order and can short circuit', async () => {
  const order = [];
  const chain = new InterceptorRegistry();
  chain.use(async (c, next) => { order.push('a'); return next(); });
  chain.use({ around: async (c, next) => { order.push('b'); return next(); } });
  assert.equal(await chain.call({}, () => 'done'), 'done');
  assert.deepEqual(order, ['a', 'b']);
  chain.use(async () => 'stopped');
  assert.equal(await chain.call({}, () => 'never'), 'stopped');
});

test('utilities', () => {
  assert.equal(snakeCase('maxFailures'), 'max_failures');
  assert.deepEqual(stringifyKeys({ a: [{ b: 1 }] }), { a: [{ b: 1 }] });
  assert.equal(sha1('x').length, 40);
});

test('gateway pool rejects unsafe arguments and unreachable sockets', async () => {
  const pool = new GatewayPool('/tmp/definitely-missing.sock');
  await assert.rejects(pool.call('SET', 'k', true), /argument/i);
  await assert.rejects(pool.call('GET', 'k'), RedisConnectionError);
  pool.close();
});

test('task registry: names, conflicts and exports scanning', () => {
  class Alpha extends Task {}
  class Named extends Task { static taskName = 'custom.Named'; }
  assert.equal(taskNameOf(Alpha), 'Alpha');
  assert.equal(taskNameOf(Named), 'custom.Named');
  assert.deepEqual(registerExports({ Alpha, Named, other: 1 }).map(taskNameOf).sort(), ['Alpha', 'custom.Named']);
  const Impostor = class Alpha2 extends Task { static taskName = 'Alpha'; };
  assert.throws(() => register(Impostor), InvalidJobError);
});

test('publication barrier entries and lua scripts are stable', () => {
  const entry = publicationBarrier.prepare({ publication: 'p', owner: 'o' });
  assert.equal(entry.key, 'solid_jobs:publication:p');
  assert.match(RESERVE, /_solid_jobs_publication/);
  assert.match(ACK, /claim_token/);
});
