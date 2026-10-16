'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { setup, teardown, waitFor } = require('./helpers');
const { Conductor } = require('../lib');
const jobs = require('./support/jobs');

test.afterEach(teardown);

for (const prefetch of [false, true]) {
  test(`a killed sidecar is restarted on the same socket and no job is lost (prefetch=${prefetch})`, async () => {
    const config = await setup({ concurrency: 4 });
    config.prefetch = prefetch;
    config.gatewayProcesses = 2;
    config.requires = [path.join(__dirname, 'support', 'jobs.js')];
    config.pollIntervalAverage = 0.2;
    const sockets = [...config.gatewaySockets];
    await jobs.Slow.enqueueMany(Array.from({ length: 300 }, (_, i) => [20, i]));
    const conductor = await new Conductor(config).start();
    await waitFor(async () => Number(await (await config.connect()).call('SCARD', 'test:done').catch(() => 0)) > 20);
    const listing = execFileSync('ps', ['-axo', 'pid=,command=']).toString().split('\n');
    const victim = listing.find((line) => line.includes('solid_redis_gateway') && line.includes(sockets[1]));
    assert.ok(victim, 'sidecar process found');
    process.kill(Number(victim.trim().split(/\s+/)[0]), 'SIGKILL');
    await waitFor(async () => Number(await (await config.connect()).call('SCARD', 'test:done').catch(() => 0)) === 300,
      { timeout: 30000 });
    assert.deepEqual(config.gatewaySockets, sockets);
    await conductor.stop();
  });
}
