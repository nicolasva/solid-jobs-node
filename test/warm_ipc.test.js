'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { EventEmitter } = require('node:events');
const { setup, teardown, waitFor } = require('./helpers');
const { Conductor, IntegrityCheck } = require('../lib');
const { ProxyPool, WarmIPCGateway } = require('../lib/warm_ipc');
const { RedisCommandError, RedisConnectionError } = require('../lib/errors');
const keyspace = require('../lib/keyspace');
const jobs = require('./support/jobs');

test.afterEach(teardown);

class FakePort extends EventEmitter {
  constructor() { super(); this.sent = []; }
  postMessage(message) { this.sent.push(message); }
}

test('ProxyPool matches replies by id and maps error classes', async () => {
  const port = new FakePort();
  const pool = new ProxyPool(port, 200);
  const first = pool.call('GET', 'a');
  const second = pool.pipelined((p) => { p.call('SET', 'a', 1); });
  await new Promise((r) => setImmediate(r));
  const [one, two] = port.sent;
  assert.deepEqual(one.payload, { command: 'GET', args: ['a'] });
  assert.deepEqual(two.payload, { pipeline: [['SET', 'a', '1']] });
  assert.equal(pool.receive({ type: 'ipc-result', id: two.id, ok: true, data: ['OK'] }), true);
  assert.equal(pool.receive({ type: 'ipc-result', id: one.id, ok: false, error: { name: 'RedisCommandError', message: 'bad' } }), true);
  assert.deepEqual(await second, ['OK']);
  await assert.rejects(first, RedisCommandError);
  assert.equal(pool.receive('stop'), false);
});

test('ProxyPool times out and rejects after close', async () => {
  const pool = new ProxyPool(new FakePort(), 30);
  await assert.rejects(pool.call('GET', 'a'), RedisConnectionError);
  pool.close();
  await assert.rejects(pool.call('GET', 'a'), /closed/);
});

test('gateway forwards worker requests over the single connection', async () => {
  const config = await setup();
  const gateway = new WarmIPCGateway(await config.connect());
  const worker = new FakePort();
  gateway.attach(worker);
  worker.emit('message', { type: 'ipc', id: 'w1', payload: { command: 'SET', args: ['k', 'v'] } });
  await waitFor(() => worker.sent.length === 1);
  assert.deepEqual(worker.sent[0], { type: 'ipc-result', id: 'w1', ok: true, data: 'OK' });
  worker.emit('message', { type: 'ipc', id: 'w2', payload: { command: 'NOPE', args: [] } });
  await waitFor(() => worker.sent.length === 2);
  assert.equal(worker.sent[1].ok, false);
  assert.equal(worker.sent[1].error.name, 'RedisCommandError');
});

test('conductor in warm IPC mode runs jobs, replays a crash and loses nothing', async () => {
  const config = await setup({ concurrency: 3 });
  config.warmIpc = true;
  config.requires = [path.join(__dirname, 'support', 'jobs.js')];
  config.pollIntervalAverage = 0.2;
  const redis = await config.connect();
  const ids = await jobs.Record.enqueueMany(Array.from({ length: 100 }, (_, i) => [i]));
  await jobs.Crasher.enqueue('w');
  const conductor = await new Conductor(config).start();
  await waitFor(async () => Number(await redis.call('SCARD', 'test:done')) === 101, { timeout: 30000 });
  assert.ok(conductor.stats().restarts >= 1);
  await conductor.stop();
  const live = await (await config.connect()).call('LLEN', keyspace.channel('default'));
  assert.equal(live, 0);
  const report = await IntegrityCheck.call({ expectedJobIds: [], ackedJobIds: ids, config });
  assert.deepEqual(report.invalidReservations, []);
});
