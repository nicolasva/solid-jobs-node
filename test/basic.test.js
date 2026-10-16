'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { setup, teardown } = require('./helpers');
const { Task, Lab, Publisher, Channel, Counters, PlannedTasks, InterceptorRegistry, invalidJsonValue } = Object.assign(
  {}, require('../lib'), { invalidJsonValue: require('../lib/publisher').invalidJsonValue });
const gateway = require('../lib/gateway');
const keyspace = require('../lib/keyspace');
const { Record, Sum } = require('./support/jobs');
const { InvalidArgumentError, RedisCommandError, InvalidJobError } = require('../lib/errors');

test.afterEach(teardown);

test('sidecar socket is 0600 and round trips commands', async () => {
  const config = await setup();
  const mode = fs.statSync(config.gatewaySocket).mode & 0o777;
  assert.equal(mode & 0o077, 0);
  const redis = await config.connect();
  assert.equal(await redis.call('SET', 'k', 'v'), 'OK');
  assert.equal(await redis.call('GET', 'k'), 'v');
  assert.equal(await redis.call('GET', 'missing'), null);
});

test('pipelines return ordered replies and concurrent calls stay matched', async () => {
  const config = await setup();
  const redis = await config.connect();
  const replies = await redis.pipelined((p) => { p.call('SET', 'a', '1'); p.call('INCR', 'a'); p.call('GET', 'a'); });
  assert.deepEqual(replies, ['OK', 2, '2']);
  const results = await Promise.all(Array.from({ length: 200 }, (_, i) => redis.call('ECHO', `v${i}`)));
  assert.deepEqual(results, Array.from({ length: 200 }, (_, i) => `v${i}`));
});

test('redis errors surface as RedisCommandError and large payloads survive', async () => {
  const config = await setup();
  const redis = await config.connect();
  await assert.rejects(redis.call('NOPE'), RedisCommandError);
  const big = 'x'.repeat(500_000);
  await redis.call('SET', 'big', big);
  assert.equal((await redis.call('GET', 'big')).length, big.length);
});

test('sidecar is shared per redis url', async () => {
  const config = await setup();
  assert.equal(await gateway.ensure(config.redisUrl, config.rubyCommand), config.gatewaySocket);
});

test('enqueue stores the envelope in the channel list', async () => {
  const config = await setup();
  const id = await Record.enqueue(1, { a: [1, 'x', null] });
  const redis = await config.connect();
  const [raw] = await redis.call('LRANGE', keyspace.channel('default'), 0, -1);
  const envelope = JSON.parse(raw);
  assert.equal(envelope.id, id);
  assert.equal(envelope.task, 'Record');
  assert.deepEqual(envelope.arguments, [1, { a: [1, 'x', null] }]);
  assert.equal(envelope.max_failures, 25);
  assert.ok(envelope.queued_ms && envelope.created_ms);
  assert.equal(await redis.call('KEYS', `${keyspace.PUBLICATION_PREFIX}*`).then((k) => k.length), 0);
});

test('task options flow into the envelope as snake_case', async () => {
  const config = await setup();
  class Custom extends Task {
    static channel = 'critical';
    static maxFailures = 7;
    static retryWithin = 60;
    async executeTask() {}
  }
  await Custom.enqueue();
  const [raw] = await (await config.connect()).call('LRANGE', keyspace.channel('critical'), 0, -1);
  const envelope = JSON.parse(raw);
  assert.equal(envelope.max_failures, 7);
  assert.equal(envelope.retry_within, 60);
});

test('enqueueMany chunks and returns ids; enqueueAfter plans the job', async () => {
  const config = await setup();
  const ids = await Record.enqueueMany([[1], [2], [3], [4], [5]], { chunkSize: 2 });
  assert.equal(ids.length, 5);
  assert.equal(await new Channel('default', config).size(), 5);
  await Record.enqueueAfter(30, 9);
  const planned = await new PlannedTasks(config).tasks();
  assert.equal(planned.length, 1);
  assert.ok(planned[0].score > Date.now() / 1000 + 25);
  assert.equal(await new Counters(config).planned(), 1);
});

test('rejects non JSON-native arguments by default', async () => {
  const config = await setup();
  await assert.rejects(Record.enqueue(() => 1), InvalidArgumentError);
  await assert.rejects(Record.enqueue(new Date()), InvalidArgumentError);
  config.onComplexArguments = 'ignore';
  await Record.enqueue(new Date());
  assert.equal(invalidJsonValue([1, 'a', { b: null }]), undefined);
});

test('rejects bad envelopes', async () => {
  await setup();
  await assert.rejects(new Publisher().publish({ task: '', arguments: [] }), InvalidJobError);
  await assert.rejects(new Publisher().publish({ task: 'X', arguments: 'nope' }), InvalidJobError);
});

test('lab capture keeps tasks out of Redis; execute runs inline', async () => {
  const config = await setup();
  await Lab.capture(async () => {
    await Record.enqueue(1);
    await Record.enqueue(2);
  });
  assert.equal(Record.captured().length, 2);
  assert.equal(await new Channel('default', config).size(), 0);
  Record.clearCaptured();
  assert.equal(await Sum.execute(1, 2, 3), 6);
  await Lab.inline(() => Sum.enqueue(4, 5));
});

test('publish interceptors can drop or pass through', async () => {
  const config = await setup();
  config.publishInterceptors.use(async (context, proceed) => (context.envelope.arguments[0] === 'drop' ? null : proceed()));
  assert.equal(await Record.enqueue('drop'), null);
  assert.ok(await Record.enqueue('keep'));
  assert.equal(await new Channel('default', config).size(), 1);
  assert.ok(config.publishInterceptors instanceof InterceptorRegistry);
});

test('withOptions overrides channel and delay', async () => {
  const config = await setup();
  await Record.withOptions({ channel: 'low' }).enqueue(1);
  assert.equal(await new Channel('low', config).size(), 1);
  await Record.withOptions({ delay: 60 }).enqueue(2);
  assert.equal(await new PlannedTasks(config).size(), 1);
});
