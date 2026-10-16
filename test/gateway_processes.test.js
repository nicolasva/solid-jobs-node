'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { REDIS_URL, teardown, waitFor } = require('./helpers');
const settings = require('../lib/settings');
const { Blueprint } = require('../lib/blueprint');
const { Conductor } = require('../lib');
const gateway = require('../lib/gateway');
const keyspace = require('../lib/keyspace');
const jobs = require('./support/jobs');

test.afterEach(async () => {
  settings.reset();
  await gateway.shutdown();
  await teardown();
});

async function multiSidecarConfig(concurrency, processes) {
  settings.reset();
  const config = settings.useConfig(new Blueprint({ redisUrl: REDIS_URL, concurrency }));
  config.logger = { level: 'silent', debug() {}, info() {}, warn() {}, error() {} };
  config.gatewayProcesses = processes;
  const redis = await config.connect();
  await redis.call('FLUSHDB');
  return config;
}

test('gatewayProcesses starts independent 0600 sidecars and shares them per index', async () => {
  const config = await multiSidecarConfig(4, 3);
  assert.equal(config.gatewaySockets.length, 3);
  assert.equal(new Set(config.gatewaySockets).size, 3);
  for (const socket of config.gatewaySockets) assert.equal(fs.statSync(socket).mode & 0o077, 0);
  assert.equal(config.gatewaySocket, config.gatewaySockets[0]);
  assert.equal(await gateway.ensure(config.redisUrl, config.rubyCommand, 1), config.gatewaySockets[1]);
  assert.equal(config.snapshot().gatewaySockets.length, 3);
});

test('workers spread over several sidecars process every job exactly once', async () => {
  const config = await multiSidecarConfig(4, 2);
  config.requires = [path.join(__dirname, 'support', 'jobs.js')];
  config.pollIntervalAverage = 0.2;
  const redis = await config.connect();
  await jobs.Record.enqueueMany(Array.from({ length: 60 }, (_, i) => [i]));
  const conductor = await new Conductor(config).start();
  await waitFor(async () => Number(await redis.call('SCARD', 'test:done')) === 60, { timeout: 30000 });
  await conductor.stop();
  assert.equal(Number(await (await config.connect()).call('LLEN', keyspace.channel('default'))), 0);
  const sockets = config.gatewaySockets;
  await gateway.shutdown();
  for (const socket of sockets) assert.equal(fs.existsSync(socket), false);
});
