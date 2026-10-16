'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { setup, teardown, waitFor } = require('./helpers');
const { Conductor, IntegrityCheck } = require('../lib');
const keyspace = require('../lib/keyspace');
const jobs = require('./support/jobs');

test.afterEach(teardown);

async function claimedEntries(redis) {
  const keys = (await redis.call('KEYS', `${keyspace.PREFIX}:node:*:claimed:*`)) || [];
  let total = 0;
  for (const key of keys) total += Number(await redis.call('LLEN', key));
  return total;
}

test('prefetch holds at most one extra job per worker and loses nothing', async () => {
  const config = await setup({ concurrency: 2 });
  config.prefetch = true;
  config.requires = [path.join(__dirname, 'support', 'jobs.js')];
  config.pollIntervalAverage = 0.2;
  const redis = await config.connect();
  await jobs.Slow.enqueueMany(Array.from({ length: 12 }, (_, i) => [150, i]));
  const conductor = await new Conductor(config).start();
  let peak = 0;
  await waitFor(async () => {
    peak = Math.max(peak, await claimedEntries(redis));
    return Number(await redis.call('SCARD', 'test:done')) === 12;
  }, { timeout: 30000, interval: 10 });
  assert.ok(peak <= 4, `at most 2 claimed jobs per worker, saw ${peak}`);
  assert.ok(peak >= 3, `prefetch should hold a second job, saw ${peak}`);
  await conductor.stop();
  assert.equal(await claimedEntries(await config.connect()), 0);
});

test('prefetch replays a crash and keeps every job exactly once', async () => {
  const config = await setup({ concurrency: 3 });
  config.prefetch = true;
  config.requires = [path.join(__dirname, 'support', 'jobs.js')];
  config.pollIntervalAverage = 0.2;
  const redis = await config.connect();
  const ids = await jobs.Record.enqueueMany(Array.from({ length: 100 }, (_, i) => [i]));
  await jobs.Crasher.enqueue('w');
  const conductor = await new Conductor(config).start();
  await waitFor(async () => Number(await redis.call('SCARD', 'test:done')) === 101, { timeout: 30000 });
  assert.ok(conductor.stats().restarts >= 1);
  await conductor.stop();
  const fresh = await config.connect();
  assert.equal(Number(await fresh.call('LLEN', keyspace.channel('default'))), 0);
  assert.equal(await claimedEntries(fresh), 0);
  const report = await IntegrityCheck.call({ expectedJobIds: [], ackedJobIds: ids, config });
  assert.deepEqual(report.invalidReservations, []);
});
