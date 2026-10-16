'use strict';

const settings = require('../lib/settings');
const { Blueprint } = require('../lib/blueprint');
const { sleep } = require('../lib/utilities');

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6390/15';

// Fresh configuration + flushed database for each test.
async function setup(options = {}) {
  settings.reset();
  const config = settings.useConfig(new Blueprint({ redisUrl: REDIS_URL, ...options }));
  config.logger = { level: 'silent', debug() {}, info() {}, warn() {}, error() {} };
  const redis = await config.connect();
  await redis.call('FLUSHDB');
  return config;
}

async function teardown() {
  settings.reset();
}

async function waitFor(predicate, { timeout = 15000, interval = 50 } = {}) {
  const deadline = Date.now() + timeout;
  for (;;) {
    const value = await predicate();
    if (value) return value;
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await sleep(interval);
  }
}

module.exports = { REDIS_URL, setup, teardown, waitFor };
