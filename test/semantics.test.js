'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { setup, teardown } = require('./helpers');
const { Claim } = require('../lib/claim');
const recovery = require('../lib/recovery');
const keyspace = require('../lib/keyspace');
const { FailurePolicy } = require('../lib/failure_policy');
const { Timer } = require('../lib/timer');
const { IntegrityCheck, DiscardedTasks, Channel } = require('../lib');
const { Record, Doomed } = require('./support/jobs');

test.afterEach(teardown);

const claimFor = (config, identity = 'host:1:abc', processorId = 0) => new Claim(config, { identity, processorId });

test('reserve, then ack removes the claim and metadata', async () => {
  const config = await setup();
  await Record.enqueue(1);
  const claim = claimFor(config);
  const record = await claim.next();
  assert.equal(record.envelope.task, 'Record');
  assert.equal(record.attempt, 1);
  const redis = await config.connect();
  assert.equal(await redis.call('LLEN', record.claimedKey), 1);
  assert.equal(await record.complete(), true);
  assert.equal(await redis.call('LLEN', record.claimedKey), 0);
  assert.equal(await redis.call('HLEN', record.claimsKey), 0);
  assert.equal(await redis.call('HLEN', keyspace.ATTEMPTS), 0);
});

test('stale ack is fenced after the job was requeued and re-reserved', async () => {
  const config = await setup();
  await Record.enqueue(1);
  const first = await claimFor(config, 'host:1:abc', 0).next();
  assert.equal(await first.requeue(), true);
  const second = await claimFor(config, 'host:2:def', 1).next();
  assert.equal(second.envelope.id, first.envelope.id);
  assert.equal(await first.complete(), false);
  assert.equal(await second.complete(), true);
});

test('a restarted processor re-adopts the claim it left behind', async () => {
  const config = await setup();
  await Record.enqueue(1);
  const before = await claimFor(config).next();
  const again = await claimFor(config).next();
  assert.equal(again.envelope.id, before.envelope.id);
  assert.equal(again.claimToken, before.claimToken);
});

test('deferred ack is sent with the next reservation', async () => {
  const config = await setup();
  await Record.enqueue(1);
  await Record.enqueue(2);
  const claim = claimFor(config);
  const first = await claim.next();
  let acked = null;
  await claim.deferAck(first, (value) => { acked = value; });
  assert.equal(acked, null);
  const second = await claim.next();
  assert.equal(acked, true);
  assert.notEqual(second.envelope.id, first.envelope.id);
  await claim.flush();
});

test('claims of dead nodes are requeued by recovery', async () => {
  const config = await setup();
  await Record.enqueue(1);
  const dead = claimFor(config, 'otherhost:99999:dead', 0);
  const record = await dead.next();
  assert.ok(record);
  assert.equal(await recovery.call(config), 1);
  const channel = new Channel('default', config);
  assert.equal(await channel.size(), 1);
  assert.equal(await (await config.connect()).call('KEYS', `${keyspace.PREFIX}:node:*`).then((k) => k.length), 0);
  const report = await IntegrityCheck.call({ expectedJobIds: [record.envelope.id], config });
  assert.equal(report.ok(), true, JSON.stringify(report));
});

test('live nodes are not recovered', async () => {
  const config = await setup();
  await Record.enqueue(1);
  const identity = `${require('node:os').hostname()}:${process.pid}:abc`;
  await claimFor(config, identity).next();
  await (await config.connect()).call('HSET', keyspace.node(identity), 'beat', String(Date.now() / 1000));
  assert.equal(await recovery.call(config), 0);
});

test('final failure archives the job and calls afterFinalFailure', async () => {
  const config = await setup();
  const id = await Doomed.enqueue();
  const record = await claimFor(config).next();
  const next = await FailurePolicy.call({ config, payload: record.envelope, error: new Error('doomed') });
  assert.equal(next, null);
  const [discarded] = await new DiscardedTasks(config).tasks();
  assert.equal(discarded.id, id);
  assert.equal(discarded.envelope.failure_count, 1);
  assert.equal(discarded.envelope.exception_message, 'doomed');
  assert.equal(await (await config.connect()).call('GET', 'test:final_failure'), `${id}:doomed`);
});

test('retry schedules into the retries set and the timer promotes due jobs', async () => {
  const config = await setup();
  config.defaultTaskOptions.max_failures = 3;
  await Record.enqueue(1);
  const record = await claimFor(config).next();
  const at = await FailurePolicy.call({ config, payload: record.envelope, error: new Error('x') });
  assert.ok(at > Date.now() / 1000);
  const redis = await config.connect();
  assert.equal(await redis.call('ZCARD', keyspace.RETRIES), 1);
  assert.equal(await new Timer(config).enqueueDue(at - 10), 0);
  assert.equal(await new Timer(config).enqueueDue(at + 1), 1);
  assert.equal(await new Channel('default', config).size(), 1);
});

test('weighted channels are visited proportionally and paused channels skipped', async () => {
  const config = await setup({ channels: [['a', 3], ['b', 1]] });
  const claim = claimFor(config);
  const seen = [];
  for (let i = 0; i < 8; i += 1) {
    claim.nextChannel(claim.channels);
    seen.push(claim.channels[(claim.channelIndex - 1) % claim.channels.length]);
  }
  assert.equal(seen.filter((c) => c.endsWith(':a')).length, 6);
  await new Channel('a', config).pause();
  await claim.refreshPaused();
  assert.deepEqual([...new Set(claim.availableChannels)], [keyspace.channel('b')]);
});
