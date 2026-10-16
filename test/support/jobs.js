'use strict';

const { Task } = require('../../lib');

const results = [];

class Record extends Task {
  async executeTask(value) {
    const redis = await require('../../lib').config().connect();
    await redis.call('SADD', 'test:done', String(value));
  }
}

class Flaky extends Task {
  static maxFailures = 3;

  static async retryDelay() {
    return 0.1;
  }

  async executeTask(key) {
    const redis = await require('../../lib').config().connect();
    const attempts = await redis.call('INCR', `test:flaky:${key}`);
    if (attempts < 3) throw new Error(`boom ${attempts}`);
    await redis.call('SADD', 'test:done', key);
  }
}

class Doomed extends Task {
  static maxFailures = 0;

  static async afterFinalFailure(envelope, error) {
    const redis = await require('../../lib').config().connect();
    await redis.call('SET', 'test:final_failure', `${envelope.id}:${error.message}`);
  }

  async executeTask() {
    throw new Error('doomed');
  }
}

// Kills the whole worker thread the first time it runs (crash before ack), succeeds when replayed.
class Crasher extends Task {
  async executeTask(value) {
    const redis = await require('../../lib').config().connect();
    const seen = await redis.call('INCR', `test:crash:${value}`);
    if (seen === 1) setTimeout(() => { throw new Error('thread crash'); }, 0);
    if (seen === 1) await new Promise((resolve) => setTimeout(resolve, 1000));
    await redis.call('SADD', 'test:done', String(value));
  }
}

class Slow extends Task {
  async executeTask(ms, value) {
    await new Promise((resolve) => setTimeout(resolve, ms));
    const redis = await require('../../lib').config().connect();
    await redis.call('SADD', 'test:done', String(value));
  }
}

class Sum extends Task {
  async executeTask(...numbers) {
    return numbers.reduce((a, b) => a + b, 0);
  }
}

module.exports = { Record, Flaky, Doomed, Crasher, Slow, Sum, results };
