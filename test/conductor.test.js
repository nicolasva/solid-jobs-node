'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { setup, teardown, waitFor } = require('./helpers');
const { Conductor, IntegrityCheck, DiscardedTasks } = require('../lib');
const keyspace = require('../lib/keyspace');
const jobs = require('./support/jobs');

const JOBS_FILE = path.join(__dirname, 'support', 'jobs.js');

async function boot(options = {}) {
  const config = await setup({ concurrency: 2, ...options });
  config.requires = [JOBS_FILE];
  config.pollIntervalAverage = 0.2;
  return config;
}

const done = async (config) => Number(await (await config.connect()).call('SCARD', 'test:done'));

test.afterEach(teardown);

test('workers execute jobs and stop cleanly', async () => {
  const config = await boot();
  const ids = await jobs.Record.enqueueMany(Array.from({ length: 30 }, (_, i) => [i]));
  const conductor = await new Conductor(config).start();
  await waitFor(async () => (await done(config)) === 30);
  const results = await conductor.stop();
  assert.deepEqual(results.map((r) => r.status), ['stopped', 'stopped']);
  const report = await IntegrityCheck.call({ expectedJobIds: [], ackedJobIds: ids, config });
  assert.deepEqual(report.invalidReservations, []);
  assert.deepEqual(report.lost, []);
});

test('no job runs twice when many workers start together', async () => {
  const config = await boot({ concurrency: 4 });
  const redis = await config.connect();
  await jobs.Record.enqueueMany(Array.from({ length: 200 }, (_, i) => [i]));
  const conductor = await new Conductor(config).start();
  await waitFor(async () => (await redis.call('LLEN', keyspace.channel('default'))) === 0);
  await conductor.stop();
  assert.equal(await done(config), 200);
  assert.equal(Number(await (await config.connect()).call('GET', keyspace.PROCESSED)), 200);
});

test('failing jobs retry then succeed', async () => {
  const config = await boot();
  config.retryBaseDelay = 0.05;
  await jobs.Flaky.enqueue('k');
  const conductor = await new Conductor(config).start();
  await waitFor(async () => (await done(config)) === 1);
  await conductor.stop();
  assert.equal(Number(await (await config.connect()).call('GET', 'test:flaky:k')), 3);
});

test('exhausted jobs land in the discarded set', async () => {
  const config = await boot();
  await jobs.Doomed.enqueue();
  const conductor = await new Conductor(config).start();
  await waitFor(async () => (await new DiscardedTasks(config).size()) === 1);
  await conductor.stop();
  assert.equal(Number(await (await config.connect()).call('GET', keyspace.FAILED)), 1);
});

test('planned jobs are promoted by the scheduler', async () => {
  const config = await boot();
  await jobs.Record.enqueueAfter(0.3, 'later');
  const conductor = await new Conductor(config).start();
  await waitFor(async () => (await done(config)) === 1);
  await conductor.stop();
});

test('a worker crash respawns the thread and replays the job (no loss)', async () => {
  const config = await boot({ concurrency: 1 });
  await jobs.Crasher.enqueue('c1');
  const conductor = await new Conductor(config).start();
  await waitFor(async () => (await done(config)) === 1, { timeout: 20000 });
  assert.ok(conductor.stats().restarts >= 1);
  await conductor.stop();
  const report = await IntegrityCheck.call({ expectedJobIds: [], ackedJobIds: [], config });
  assert.deepEqual(report.invalidReservations, []);
  assert.equal(await (await config.connect()).call('LLEN', keyspace.channel('default')), 0);
});

test('graceful stop drains in-flight jobs', async () => {
  const config = await boot({ concurrency: 1 });
  await jobs.Slow.enqueue(800, 'slow');
  const conductor = await new Conductor(config).start();
  const redis = await config.connect();
  await waitFor(async () => (await redis.call('LLEN', keyspace.claimed(conductor.identity, 0))) === 1);
  await conductor.stop();
  assert.equal(await done(config), 1);
});

test('quiet stops new reservations', async () => {
  const config = await boot({ concurrency: 1 });
  const conductor = await new Conductor(config).start();
  await conductor.quiet();
  await new Promise((resolve) => setTimeout(resolve, 300));
  await jobs.Record.enqueue('q');
  await new Promise((resolve) => setTimeout(resolve, 1000));
  assert.equal(await done(config), 0);
  await conductor.stop();
});

test('heartbeat publishes the node and removes it on stop', async () => {
  const config = await boot();
  const conductor = await new Conductor(config).start();
  const redis = await config.connect();
  assert.equal(Number(await redis.call('HGET', keyspace.node(conductor.identity), 'concurrency')), 2);
  const { identity } = conductor;
  await conductor.stop();
  assert.equal(await (await config.connect()).call('EXISTS', keyspace.node(identity)), 0);
});

test('a worker that fails to boot aborts start()', async () => {
  const config = await boot();
  config.requires = ['/nonexistent/file.js'];
  await assert.rejects(new Conductor(config).start(), /failed to boot/);
});
