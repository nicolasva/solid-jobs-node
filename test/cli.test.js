'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');
const { setup, teardown, waitFor, REDIS_URL } = require('./helpers');
const { parse, applyOptions, parseChannel } = require('../lib/console');
const { Blueprint } = require('../lib/blueprint');
const jobs = require('./support/jobs');

const BIN = path.join(__dirname, '..', 'bin', 'solid-jobs-node');
const JOBS_FILE = path.join(__dirname, 'support', 'jobs.js');

test.afterEach(teardown);

test('parses channels, weights and options', () => {
  assert.deepEqual(parseChannel('critical,3'), ['critical', 3]);
  assert.equal(parseChannel('default'), 'default');
  const config = applyOptions(new Blueprint(), parse(['-c', '4', '--channel', 'critical,3', '--channel', 'default', '-t', '9', '-r', './x.js']));
  assert.equal(config.concurrency, 4);
  assert.deepEqual(config.channelEntries, [['critical', 3], ['default', 1]]);
  assert.equal(config.shutdownTimeout, 9);
  assert.deepEqual(config.requires, ['./x.js']);
});

test('--version and --help exit 0', () => {
  assert.equal(spawnSync(BIN, ['-v']).status, 0);
  assert.match(spawnSync(BIN, ['-h']).stdout.toString(), /--concurrency/);
});

test('CLI runs jobs, pauses on SIGTSTP and stops on SIGTERM', async () => {
  const config = await setup();
  const redis = await config.connect();
  await jobs.Record.enqueueMany(Array.from({ length: 10 }, (_, i) => [i]));
  const child = spawn(BIN, ['-r', JOBS_FILE, '-c', '2'], { env: { ...process.env, REDIS_URL, SOLID_JOBS_LOG_LEVEL: 'error' }, stdio: ['ignore', 'inherit', 'inherit'] });
  const exited = new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));
  await waitFor(async () => Number(await redis.call('SCARD', 'test:done')) === 10);

  child.kill('SIGTSTP');
  await waitFor(async () => (await redis.call('HGET', `solid_jobs:node:${(await redis.call('SMEMBERS', 'solid_jobs:nodes'))[0]}`, 'quiet')) === 'true', { timeout: 15000 });

  child.kill('SIGTERM');
  const result = await exited;
  assert.equal(result.code, 0);
  assert.equal(await redis.call('SCARD', 'solid_jobs:nodes'), 0);
});
